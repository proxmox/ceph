/*
 * rbd-wnbd - RBD in userspace
 *
 * Copyright (C) 2020 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#include "include/int_types.h"

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/locale/encoding_utf.hpp>

#include "wnbd_handler.h"
#include "rbd_wnbd.h"

#include <fstream>
#include <memory>
#include <regex>

#include "common/Formatter.h"
#include "common/TextTable.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/version.h"
#include "common/win32/service.h"
#include "common/admin_socket_client.h"

#include "global/global_init.h"

#include "include/uuid.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"

#include <shellapi.h>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "rbd-wnbd: "

using namespace std;
using boost::locale::conv::utf_to_utf;

std::wstring to_wstring(const std::string& str)
{
  return utf_to_utf<wchar_t>(str.c_str(), str.c_str() + str.size());
}

std::string to_string(const std::wstring& str)
{
  return utf_to_utf<char>(str.c_str(), str.c_str() + str.size());
}

bool is_process_running(DWORD pid)
{
  HANDLE process = OpenProcess(SYNCHRONIZE, FALSE, pid);
  DWORD ret = WaitForSingleObject(process, 0);
  CloseHandle(process);
  return ret == WAIT_TIMEOUT;
}

DWORD WNBDActiveDiskIterator::fetch_list(
  PWNBD_CONNECTION_LIST* conn_list)
{
  DWORD curr_buff_sz = 0;
  DWORD buff_sz = 0;
  DWORD err = 0;
  PWNBD_CONNECTION_LIST tmp_list = NULL;

  // We're using a loop because other connections may show up by the time
  // we retry.
  do {
    if (tmp_list)
      free(tmp_list);

    if (buff_sz) {
      tmp_list = (PWNBD_CONNECTION_LIST) calloc(1, buff_sz);
      if (!tmp_list) {
        derr << "Could not allocate " << buff_sz << " bytes." << dendl;
        err = ERROR_NOT_ENOUGH_MEMORY;
        break;
      }
    }

    curr_buff_sz = buff_sz;
    // If the buffer is too small, the return value is 0 and "BufferSize"
    // will contain the required size. This is counterintuitive, but
    // Windows drivers can't return a buffer as well as a non-zero status.
    err = WnbdList(tmp_list, &buff_sz);
    if (err)
      break;
  } while (curr_buff_sz < buff_sz);

  if (err) {
    if (tmp_list)
      free(tmp_list);
  } else {
    *conn_list = tmp_list;
  }
  return err;
}

WNBDActiveDiskIterator::WNBDActiveDiskIterator()
{
  DWORD status = WNBDActiveDiskIterator::fetch_list(&conn_list);
  if (status) {
    error = EINVAL;
  }
}

WNBDActiveDiskIterator::~WNBDActiveDiskIterator()
{
  if (conn_list) {
    free(conn_list);
    conn_list = NULL;
  }
}

bool WNBDActiveDiskIterator::get(Config *cfg)
{
  index += 1;
  *cfg = Config();

  if (!conn_list || index >= (int)conn_list->Count) {
    return false;
  }

  auto conn_info = conn_list->Connections[index];
  auto conn_props = conn_info.Properties;

  if (strncmp(conn_props.Owner, RBD_WNBD_OWNER_NAME, WNBD_MAX_OWNER_LENGTH)) {
    dout(10) << "Ignoring disk: " << conn_props.InstanceName
             << ". Owner: " << conn_props.Owner << dendl;
    return this->get(cfg);
  }

  error = load_mapping_config_from_registry(conn_props.InstanceName, cfg);
  if (error) {
    derr << "Could not load registry disk info for: "
         << conn_props.InstanceName << ". Error: " << error << dendl;
    return false;
  }

  cfg->disk_number = conn_info.DiskNumber;
  cfg->serial_number = std::string(conn_props.SerialNumber);
  cfg->pid = conn_props.Pid;
  cfg->active = cfg->disk_number > 0 && is_process_running(conn_props.Pid);
  cfg->wnbd_mapped = true;

  return true;
}

RegistryDiskIterator::RegistryDiskIterator()
{
  reg_key = new RegistryKey(g_ceph_context, HKEY_LOCAL_MACHINE,
                            SERVICE_REG_KEY, false);
  if (!reg_key->hKey) {
    if (!reg_key->missingKey)
      error = EINVAL;
    return;
  }

  if (RegQueryInfoKey(reg_key->hKey, NULL, NULL, NULL, &subkey_count,
                     NULL, NULL, NULL, NULL, NULL, NULL, NULL)) {
    derr << "Could not query registry key: " << SERVICE_REG_KEY << dendl;
    error = EINVAL;
    return;
  }
}

bool RegistryDiskIterator::get(Config *cfg)
{
  index += 1;
  *cfg = Config();

  if (!reg_key->hKey || !subkey_count) {
    return false;
  }

  char subkey_name[MAX_PATH] = {0};
  DWORD subkey_name_sz = MAX_PATH;
  int err = RegEnumKeyEx(
    reg_key->hKey, index, subkey_name, &subkey_name_sz,
    NULL, NULL, NULL, NULL);
  if (err == ERROR_NO_MORE_ITEMS) {
    return false;
  } else if (err) {
    derr << "Could not enumerate registry. Error: " << err << dendl;
    error = EINVAL;
    return false;
  }

  if (load_mapping_config_from_registry(subkey_name, cfg)) {
    error = EINVAL;
    return false;
  };

  return true;
}

// Iterate over all RBD mappings, getting info from the registry and the driver.
bool WNBDDiskIterator::get(Config *cfg)
{
  *cfg = Config();

  bool found_active = active_iterator.get(cfg);
  if (found_active) {
    active_devices.insert(cfg->devpath);
    return true;
  }

  error = active_iterator.get_error();
  if (error) {
    dout(5) << ": WNBD iterator error: " << error << dendl;
    return false;
  }

  while(registry_iterator.get(cfg)) {
    if (active_devices.find(cfg->devpath) != active_devices.end()) {
      // Skip active devices that were already yielded.
      continue;
    }
    return true;
  }

  error = registry_iterator.get_error();
  if (error) {
    dout(5) << ": Registry iterator error: " << error << dendl;
  }
  return false;
}

int get_exe_path(std::string& path) {
  char buffer[MAX_PATH];
  DWORD err = 0;

  int ret = GetModuleFileNameA(NULL, buffer, MAX_PATH);
  if (!ret || ret == MAX_PATH) {
    err = GetLastError();
    derr << "Could not retrieve executable path. "
        << "Error: " << win32_strerror(err) << dendl;
    return -EINVAL;
  }

  path = buffer;
  return 0;
}

std::string get_cli_args() {
  std::ostringstream cmdline;
  for (int i=1; i<__argc; i++) {
    if (i > 1)
      cmdline << " ";
    cmdline << std::quoted(__argv[i]);
  }
  return cmdline.str();
}

int send_map_request(std::string arguments) {
  dout(15) << __func__ << ": command arguments: " << arguments << dendl;

  BYTE request_buff[SERVICE_PIPE_BUFFSZ] = { 0 };
  ServiceRequest* request = (ServiceRequest*) request_buff;
  request->command = Connect;
  arguments.copy(
    (char*)request->arguments,
    SERVICE_PIPE_BUFFSZ - FIELD_OFFSET(ServiceRequest, arguments));
  ServiceReply reply = { 0 };

  DWORD bytes_read = 0;
  BOOL success = CallNamedPipe(
    SERVICE_PIPE_NAME,
    request_buff,
    SERVICE_PIPE_BUFFSZ,
    &reply,
    sizeof(reply),
    &bytes_read,
    DEFAULT_MAP_TIMEOUT_MS);
  if (!success) {
    DWORD err = GetLastError();
    derr << "Could not send device map request. "
         << "Make sure that the ceph service is running. "
         << "Error: " << win32_strerror(err) << dendl;
    return -EINVAL;
  }
  if (reply.status) {
    derr << "The ceph service failed to map the image. "
         << "Check the log file or pass '-f' (foreground mode) "
         << "for additional information. "
         << "Error: " << cpp_strerror(reply.status)
         << dendl;
  }

  return reply.status;
}

// Spawn a subprocess using the specified "rbd-wnbd" command
// arguments. A pipe is passed to the child process,
// which will allow it to communicate the mapping status
int map_device_using_suprocess(std::string arguments, int timeout_ms)
{
  STARTUPINFO si;
  PROCESS_INFORMATION pi;
  char ch;
  DWORD err = 0, status = 0;
  int exit_code = 0;
  std::ostringstream command_line;
  std::string exe_path;
  // Windows async IO context
  OVERLAPPED connect_o, read_o;
  HANDLE connect_event = NULL, read_event = NULL;
  // Used for waiting on multiple events that are going to be initialized later.
  HANDLE wait_events[2] = { INVALID_HANDLE_VALUE, INVALID_HANDLE_VALUE};
  DWORD bytes_read = 0;
  // We may get a command line containing an old pipe handle when
  // recreating mappings, so we'll have to replace it.
  std::regex pipe_pattern("([\'\"]?--pipe-name[\'\"]? +[\'\"]?[^ ]+[\'\"]?)");

  uuid_d uuid;
  uuid.generate_random();
  std::ostringstream pipe_name;
  pipe_name << "\\\\.\\pipe\\rbd-wnbd-" << uuid;

  // Create an unique named pipe to communicate with the child. */
  HANDLE pipe_handle = CreateNamedPipe(
    pipe_name.str().c_str(),
    PIPE_ACCESS_INBOUND | FILE_FLAG_FIRST_PIPE_INSTANCE |
      FILE_FLAG_OVERLAPPED,
    PIPE_WAIT,
    1, // Only accept one instance
    SERVICE_PIPE_BUFFSZ,
    SERVICE_PIPE_BUFFSZ,
    SERVICE_PIPE_TIMEOUT_MS,
    NULL);
  if (pipe_handle == INVALID_HANDLE_VALUE) {
    err = GetLastError();
    derr << "CreateNamedPipe failed: " << win32_strerror(err) << dendl;
    exit_code = -ECHILD;
    goto finally;
  }
  connect_event = CreateEvent(0, TRUE, FALSE, NULL);
  read_event = CreateEvent(0, TRUE, FALSE, NULL);
  if (!connect_event || !read_event) {
    err = GetLastError();
    derr << "CreateEvent failed: " << win32_strerror(err) << dendl;
    exit_code = -ECHILD;
    goto finally;
  }
  connect_o.hEvent = connect_event;
  read_o.hEvent = read_event;

  status = ConnectNamedPipe(pipe_handle, &connect_o);
  err = GetLastError();
  if (status || err != ERROR_IO_PENDING) {
    if (status)
      err = status;
    derr << "ConnectNamedPipe failed: " << win32_strerror(err) << dendl;
    exit_code = -ECHILD;
    goto finally;
  }
  err = 0;

  dout(5) << __func__ << ": command arguments: " << arguments << dendl;

  // We'll avoid running arbitrary commands, instead using the executable
  // path of this process (expected to be the full rbd-wnbd.exe path).
  err = get_exe_path(exe_path);
  if (err) {
    exit_code = -EINVAL;
    goto finally;
  }
  command_line << std::quoted(exe_path)
               << " " << std::regex_replace(arguments, pipe_pattern, "")
               << " --pipe-name " << pipe_name.str();

  dout(5) << __func__ << ": command line: " << command_line.str() << dendl;

  GetStartupInfo(&si);
  // Create a detached child
  if (!CreateProcess(NULL, (char*)command_line.str().c_str(),
                     NULL, NULL, FALSE, DETACHED_PROCESS,
                     NULL, NULL, &si, &pi)) {
    err = GetLastError();
    derr << "CreateProcess failed: " << win32_strerror(err) << dendl;
    exit_code = -ECHILD;
    goto finally;
  }

  wait_events[0] = connect_event;
  wait_events[1] = pi.hProcess;
  status = WaitForMultipleObjects(2, wait_events, FALSE, timeout_ms);
  switch(status) {
    case WAIT_OBJECT_0:
      if (!GetOverlappedResult(pipe_handle, &connect_o, &bytes_read, TRUE)) {
        err = GetLastError();
        derr << "Couln't establish a connection with the child process. "
             << "Error: " << win32_strerror(err) << dendl;
        exit_code = -ECHILD;
        goto clean_process;
      }
      // We have an incoming connection.
      break;
    case WAIT_OBJECT_0 + 1:
      // The process has exited prematurely.
      goto clean_process;
    case WAIT_TIMEOUT:
      derr << "Timed out waiting for child process connection." << dendl;
      goto clean_process;
    default:
      derr << "Failed waiting for child process. Status: " << status << dendl;
      goto clean_process;
  }
  // Block and wait for child to say it is ready.
  dout(5) << __func__ << ": waiting for child notification." << dendl;
  if (!ReadFile(pipe_handle, &ch, 1, NULL, &read_o)) {
    err = GetLastError();
    if (err != ERROR_IO_PENDING) {
      derr << "Receiving child process reply failed with: "
           << win32_strerror(err) << dendl;
      exit_code = -ECHILD;
      goto clean_process;
    }
  }
  wait_events[0] = read_event;
  wait_events[1] = pi.hProcess;
  // The RBD daemon is expected to write back right after opening the
  // pipe. We'll use the same timeout value for now.
  status = WaitForMultipleObjects(2, wait_events, FALSE, timeout_ms);
  switch(status) {
    case WAIT_OBJECT_0:
      if (!GetOverlappedResult(pipe_handle, &read_o, &bytes_read, TRUE)) {
        err = GetLastError();
        derr << "Receiving child process reply failed with: "
             << win32_strerror(err) << dendl;
        exit_code = -ECHILD;
        goto clean_process;
      }
      break;
    case WAIT_OBJECT_0 + 1:
      // The process has exited prematurely.
      goto clean_process;
    case WAIT_TIMEOUT:
      derr << "Timed out waiting for child process message." << dendl;
      goto clean_process;
    default:
      derr << "Failed waiting for child process. Status: " << status << dendl;
      goto clean_process;
  }

  dout(5) << __func__ << ": received child notification." << dendl;
  goto finally;

  clean_process:
    if (!is_process_running(pi.dwProcessId)) {
      GetExitCodeProcess(pi.hProcess, (PDWORD)&exit_code);
      derr << "Daemon failed with: " << cpp_strerror(exit_code) << dendl;
    } else {
      // The process closed the pipe without notifying us or exiting.
      // This is quite unlikely, but we'll terminate the process.
      dout(5) << "Terminating unresponsive process." << dendl;
      TerminateProcess(pi.hProcess, 1);
      exit_code = -EINVAL;
    }

  finally:
    if (exit_code)
      derr << "Could not start RBD daemon." << dendl;
    if (pipe_handle)
      CloseHandle(pipe_handle);
    if (connect_event)
      CloseHandle(connect_event);
    if (read_event)
      CloseHandle(read_event);
  return exit_code;
}

BOOL WINAPI console_handler_routine(DWORD dwCtrlType)
{
  dout(5) << "Received control signal: " << dwCtrlType
          << ". Exiting." << dendl;

  std::unique_lock l{shutdown_lock};
  if (handler)
    handler->shutdown();

  return true;
}

int save_config_to_registry(Config* cfg)
{
  std::string strKey{ SERVICE_REG_KEY };
  strKey.append("\\");
  strKey.append(cfg->devpath);
  auto reg_key = RegistryKey(
    g_ceph_context, HKEY_LOCAL_MACHINE, strKey.c_str(), true);
  if (!reg_key.hKey) {
      return -EINVAL;
  }

  int ret_val = 0;
  // Registry writes are immediately available to other processes.
  // Still, we'll do a flush to ensure that the mapping can be
  // recreated after a system crash.
  if (reg_key.set("pid", getpid()) ||
      reg_key.set("devpath", cfg->devpath) ||
      reg_key.set("poolname", cfg->poolname) ||
      reg_key.set("nsname", cfg->nsname) ||
      reg_key.set("imgname", cfg->imgname) ||
      reg_key.set("snapname", cfg->snapname) ||
      reg_key.set("command_line", get_cli_args()) ||
      reg_key.set("persistent", cfg->persistent) ||
      reg_key.set("admin_sock_path", g_conf()->admin_socket) ||
      reg_key.flush()) {
    ret_val = -EINVAL;
  }

  return ret_val;
}

int remove_config_from_registry(Config* cfg)
{
  std::string strKey{ SERVICE_REG_KEY };
  strKey.append("\\");
  strKey.append(cfg->devpath);
  return RegistryKey::remove(
    g_ceph_context, HKEY_LOCAL_MACHINE, strKey.c_str());
}

int load_mapping_config_from_registry(string devpath, Config* cfg)
{
  std::string strKey{ SERVICE_REG_KEY };
  strKey.append("\\");
  strKey.append(devpath);
  auto reg_key = RegistryKey(
    g_ceph_context, HKEY_LOCAL_MACHINE, strKey.c_str(), false);
  if (!reg_key.hKey) {
    if (reg_key.missingKey)
      return -ENOENT;
    else
      return -EINVAL;
  }

  reg_key.get("devpath", cfg->devpath);
  reg_key.get("poolname", cfg->poolname);
  reg_key.get("nsname", cfg->nsname);
  reg_key.get("imgname", cfg->imgname);
  reg_key.get("snapname", cfg->snapname);
  reg_key.get("command_line", cfg->command_line);
  reg_key.get("persistent", cfg->persistent);
  reg_key.get("admin_sock_path", cfg->admin_sock_path);

  return 0;
}

int restart_registered_mappings(
  int worker_count,
  int total_timeout,
  int image_map_timeout)
{
  Config cfg;
  WNBDDiskIterator iterator;
  int err = 0, r;

  int total_timeout_ms = max(total_timeout, total_timeout * 1000);
  int image_map_timeout_ms = max(image_map_timeout, image_map_timeout * 1000);

  LARGE_INTEGER start_t, counter_freq;
  QueryPerformanceFrequency(&counter_freq);
  QueryPerformanceCounter(&start_t);

  boost::asio::thread_pool pool(worker_count);
  while (iterator.get(&cfg)) {
    if (cfg.command_line.empty()) {
      derr << "Could not recreate mapping, missing command line: "
           << cfg.devpath << dendl;
      err = -EINVAL;
      continue;
    }
    if (cfg.wnbd_mapped) {
      dout(5) << __func__ << ": device already mapped: "
              << cfg.devpath << dendl;
      continue;
    }
    if (!cfg.persistent) {
      dout(5) << __func__ << ": cleaning up non-persistent mapping: "
              << cfg.devpath << dendl;
      r = remove_config_from_registry(&cfg);
      if (r) {
        derr << __func__ << ": could not clean up non-persistent mapping: "
             << cfg.devpath << dendl;
      }
      continue;
    }

    boost::asio::post(pool,
      [&, cfg]() mutable
      {
        LARGE_INTEGER curr_t, elapsed_ms;
        QueryPerformanceCounter(&curr_t);
        elapsed_ms.QuadPart = curr_t.QuadPart - start_t.QuadPart;
        elapsed_ms.QuadPart *= 1000;
        elapsed_ms.QuadPart /= counter_freq.QuadPart;

        int time_left_ms = max(
          0,
          total_timeout_ms - (int)elapsed_ms.QuadPart);
        time_left_ms = min(image_map_timeout_ms, time_left_ms);
        if (!time_left_ms) {
          err = -ETIMEDOUT;
          return;
        }

        dout(5) << "Remapping: " << cfg.devpath
                << ". Timeout: " << time_left_ms << " ms." << dendl;

        // We'll try to map all devices and return a non-zero value
        // if any of them fails.
        r = map_device_using_suprocess(cfg.command_line, time_left_ms);
        if (r) {
          err = r;
          derr << "Could not create mapping: "
               << cfg.devpath << ". Error: " << r << dendl;
        } else {
          dout(5) << "Successfully remapped: " << cfg.devpath << dendl;
        }
      });
  }
  pool.join();

  r = iterator.get_error();
  if (r) {
    derr << "Could not fetch all mappings. Error: " << r << dendl;
    err = r;
  }

  return err;
}

int disconnect_all_mappings(
  bool unregister,
  bool hard_disconnect,
  int soft_disconnect_timeout,
  int worker_count)
{
  // Although not generally recommended, soft_disconnect_timeout can be 0,
  // which means infinite timeout.
  ceph_assert(soft_disconnect_timeout >= 0);
  ceph_assert(worker_count > 0);
  int64_t timeout_ms = soft_disconnect_timeout * 1000;

  Config cfg;
  WNBDActiveDiskIterator iterator;
  int err = 0, r;

  boost::asio::thread_pool pool(worker_count);
  LARGE_INTEGER start_t, counter_freq;
  QueryPerformanceFrequency(&counter_freq);
  QueryPerformanceCounter(&start_t);
  while (iterator.get(&cfg)) {
    boost::asio::post(pool,
      [&, cfg]() mutable
      {
        LARGE_INTEGER curr_t, elapsed_ms;
        QueryPerformanceCounter(&curr_t);
        elapsed_ms.QuadPart = curr_t.QuadPart - start_t.QuadPart;
        elapsed_ms.QuadPart *= 1000;
        elapsed_ms.QuadPart /= counter_freq.QuadPart;

        int64_t time_left_ms = max((int64_t)0, timeout_ms - elapsed_ms.QuadPart);

        cfg.hard_disconnect = hard_disconnect || !time_left_ms;
        cfg.hard_disconnect_fallback = true;
        cfg.soft_disconnect_timeout = time_left_ms / 1000;

        dout(5) << "Removing mapping: " << cfg.devpath
                << ". Timeout: " << cfg.soft_disconnect_timeout
                << "s. Hard disconnect: " << cfg.hard_disconnect
                << dendl;

        r = do_unmap(&cfg, unregister);
        if (r) {
          err = r;
          derr << "Could not remove mapping: " << cfg.devpath
               << ". Error: " << r << dendl;
        } else {
          dout(5) << "Successfully removed mapping: " << cfg.devpath << dendl;
        }
      });
  }
  pool.join();

  r = iterator.get_error();
  if (r) {
    derr << "Could not fetch all mappings. Error: " << r << dendl;
    err = r;
  }

  return err;
}

class RBDService : public ServiceBase {
  private:
    bool hard_disconnect;
    int soft_disconnect_timeout;
    int thread_count;
    int service_start_timeout;
    int image_map_timeout;
    bool remap_failure_fatal;

  public:
    RBDService(bool _hard_disconnect,
               int _soft_disconnect_timeout,
               int _thread_count,
               int _service_start_timeout,
               int _image_map_timeout,
               bool _remap_failure_fatal)
      : ServiceBase(g_ceph_context)
      , hard_disconnect(_hard_disconnect)
      , soft_disconnect_timeout(_soft_disconnect_timeout)
      , thread_count(_thread_count)
      , service_start_timeout(_service_start_timeout)
      , image_map_timeout(_image_map_timeout)
      , remap_failure_fatal(_remap_failure_fatal)
    {
    }

    static int execute_command(ServiceRequest* request)
    {
      switch(request->command) {
        case Connect:
          dout(5) << "Received device connect request. Command line: "
                  << (char*)request->arguments << dendl;
          // TODO: use the configured service map timeout.
          // TODO: add ceph.conf options.
          return map_device_using_suprocess(
            (char*)request->arguments, DEFAULT_MAP_TIMEOUT_MS);
        default:
          dout(5) << "Received unsupported command: "
                  << request->command << dendl;
          return -ENOSYS;
      }
    }

    static DWORD handle_connection(HANDLE pipe_handle)
    {
      PBYTE message[SERVICE_PIPE_BUFFSZ] = { 0 };
      DWORD bytes_read = 0, bytes_written = 0;
      DWORD err = 0;
      DWORD reply_sz = 0;
      ServiceReply reply = { 0 };

      dout(20) << __func__ << ": Receiving message." << dendl;
      BOOL success = ReadFile(
        pipe_handle, message, SERVICE_PIPE_BUFFSZ,
        &bytes_read, NULL);
      if (!success || !bytes_read) {
        err = GetLastError();
        derr << "Could not read service command: "
             << win32_strerror(err) << dendl;
        goto exit;
      }

      dout(20) << __func__ << ": Executing command." << dendl;
      reply.status = execute_command((ServiceRequest*) message);
      reply_sz = sizeof(reply);

      dout(20) << __func__ << ": Sending reply. Status: "
               << reply.status << dendl;
      success = WriteFile(
        pipe_handle, &reply, reply_sz, &bytes_written, NULL);
      if (!success || reply_sz != bytes_written) {
        err = GetLastError();
        derr << "Could not send service command result: "
             << win32_strerror(err) << dendl;
      }

exit:
      dout(20) << __func__ << ": Cleaning up connection." << dendl;
      FlushFileBuffers(pipe_handle);
      DisconnectNamedPipe(pipe_handle);
      CloseHandle(pipe_handle);

      return err;
    }

    // We have to support Windows server 2016. Unix sockets only work on
    // WS 2019, so we can't use the Ceph admin socket abstraction.
    // Getting the Ceph admin sockets to work with Windows named pipes
    // would require quite a few changes.
    static DWORD accept_pipe_connection() {
      DWORD err = 0;
      // We're currently using default ACLs, which grant full control to the
      // LocalSystem account and administrator as well as the owner.
      dout(20) << __func__ << ": opening new pipe instance" << dendl;
      HANDLE pipe_handle = CreateNamedPipe(
        SERVICE_PIPE_NAME,
        PIPE_ACCESS_DUPLEX,
        PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE | PIPE_WAIT,
        PIPE_UNLIMITED_INSTANCES,
        SERVICE_PIPE_BUFFSZ,
        SERVICE_PIPE_BUFFSZ,
        SERVICE_PIPE_TIMEOUT_MS,
        NULL);
      if (pipe_handle == INVALID_HANDLE_VALUE) {
        err = GetLastError();
        derr << "CreatePipe failed: " << win32_strerror(err) << dendl;
        return -EINVAL;
      }

      dout(20) << __func__ << ": waiting for connections." << dendl;
      BOOL connected = ConnectNamedPipe(pipe_handle, NULL);
      if (!connected) {
        err = GetLastError();
        if (err != ERROR_PIPE_CONNECTED) {
          derr << "Pipe connection failed: " << win32_strerror(err) << dendl;

          CloseHandle(pipe_handle);
          return err;
        }
      }

      dout(20) << __func__ << ": Connection received." << dendl;
      // We'll handle the connection in a separate thread and at the same time
      // accept a new connection.
      HANDLE handler_thread = CreateThread(
        NULL, 0, (LPTHREAD_START_ROUTINE) handle_connection, pipe_handle, 0, 0);
      if (!handler_thread) {
        err = GetLastError();
        derr << "Could not start pipe connection handler thread: "
             << win32_strerror(err) << dendl;
        CloseHandle(pipe_handle);
      } else {
        CloseHandle(handler_thread);
      }

      return err;
    }

    static int pipe_server_loop(LPVOID arg)
    {
      dout(5) << "Accepting admin pipe connections." << dendl;
      while (1) {
        // This call will block until a connection is received, which will
        // then be handled in a separate thread. The function returns, allowing
        // us to accept another simultaneous connection.
        accept_pipe_connection();
      }
      return 0;
    }

    int create_pipe_server() {
      HANDLE handler_thread = CreateThread(
        NULL, 0, (LPTHREAD_START_ROUTINE) pipe_server_loop, NULL, 0, 0);
      DWORD err = 0;

      if (!handler_thread) {
        err = GetLastError();
        derr << "Could not start pipe server: " << win32_strerror(err) << dendl;
      } else {
        CloseHandle(handler_thread);
      }

      return err;
    }

    int run_hook() override {
      // Restart registered mappings before accepting new ones.
      int r = restart_registered_mappings(
        thread_count, service_start_timeout, image_map_timeout);
      if (r) {
        if (remap_failure_fatal) {
          derr << "Couldn't remap all images. Cleaning up." << dendl;
          return r;
        } else {
          dout(0) << "Ignoring image remap failure." << dendl;
        }
      }

      return create_pipe_server();
    }

    // Invoked when the service is requested to stop.
    int stop_hook() override {
      return disconnect_all_mappings(
        false, hard_disconnect, soft_disconnect_timeout, thread_count);
    }
    // Invoked when the system is shutting down.
    int shutdown_hook() override {
      return stop_hook();
    }
};

static void usage()
{
  const char* usage_str =R"(
Usage: rbd-wnbd [options] map <image-or-snap-spec>           Map an image to wnbd device
                [options] unmap <device|image-or-snap-spec>  Unmap wnbd device
                [options] list                               List mapped wnbd devices
                [options] show <image-or-snap-spec>          Show mapped wnbd device
                stats <image-or-snap-spec>                   Show IO counters
                [options] service                            Windows service entrypoint,
                                                             handling device lifecycle

Map options:
  --device <device path>  Optional mapping unique identifier
  --exclusive             Forbid writes by other clients
  --read-only             Map read-only
  --non-persistent        Do not recreate the mapping when the Ceph service
                          restarts. By default, mappings are persistent
  --io-req-workers        The number of workers that dispatch IO requests.
                          Default: 4
  --io-reply-workers      The number of workers that dispatch IO replies.
                          Default: 4

Unmap options:
  --hard-disconnect              Skip attempting a soft disconnect
  --no-hard-disconnect-fallback  Immediately return an error if the soft
                                 disconnect fails instead of attempting a hard
                                 disconnect as fallback
  --soft-disconnect-timeout   Soft disconnect timeout in seconds. The soft
                              disconnect operation uses PnP to notify the
                              Windows storage stack that the device is going to
                              be disconnectd. Storage drivers can block this
                              operation if there are pending operations,
                              unflushed caches or open handles. Default: 15

Service options:
  --hard-disconnect           Skip attempting a soft disconnect
  --soft-disconnect-timeout   Cummulative soft disconnect timeout in seconds,
                              used when disconnecting existing mappings. A hard
                              disconnect will be issued when hitting the timeout
  --service-thread-count      The number of workers used when mapping or
                              unmapping images. Default: 8
  --start-timeout             The service start timeout in seconds. Default: 120
  --map-timeout               Individual image map timeout in seconds. Default: 20
  --remap-failure-fatal       If set, the service will stop when failing to remap
                              an image at start time, unmapping images that have
                              been mapped so far.

Show|List options:
  --format plain|json|xml Output format (default: plain)
  --pretty-format         Pretty formatting (json and xml)

Common options:
  --wnbd-log-level        libwnbd.dll log level

)";

  std::cout << usage_str;
  generic_server_usage();
}


static Command cmd = None;

int construct_devpath_if_missing(Config* cfg)
{
  // Windows doesn't allow us to request specific disk paths when mapping an
  // image. This will just be used by rbd-wnbd and wnbd as an identifier.
  if (cfg->devpath.empty()) {
    if (cfg->imgname.empty()) {
      derr << "Missing image name." << dendl;
      return -EINVAL;
    }

    if (!cfg->poolname.empty()) {
      cfg->devpath += cfg->poolname;
      cfg->devpath += '/';
    }
    if (!cfg->nsname.empty()) {
      cfg->devpath += cfg->nsname;
      cfg->devpath += '/';
    }

    cfg->devpath += cfg->imgname;

    if (!cfg->snapname.empty()) {
      cfg->devpath += '@';
      cfg->devpath += cfg->snapname;
    }
  }

  return 0;
}

boost::intrusive_ptr<CephContext> do_global_init(
  int argc, const char *argv[], Config *cfg)
{
  auto args = argv_to_vec(argc, argv);

  code_environment_t code_env;
  int flags;

  switch(cmd) {
    case Connect:
      code_env = CODE_ENVIRONMENT_DAEMON;
      flags = CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS;
      break;
    case Service:
      code_env = CODE_ENVIRONMENT_DAEMON;
      flags = CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS |
              CINIT_FLAG_NO_MON_CONFIG |
              CINIT_FLAG_NO_DAEMON_ACTIONS;
      break;
    default:
      code_env = CODE_ENVIRONMENT_UTILITY;
      flags = CINIT_FLAG_NO_MON_CONFIG;
      break;
  }

  global_pre_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, code_env, flags);
  // Avoid cluttering the console when spawning a mapping that will run
  // in the background.
  if (g_conf()->daemonize && cfg->parent_pipe.empty()) {
    flags |= CINIT_FLAG_NO_DAEMON_ACTIONS;
  }
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         code_env, flags, FALSE);

  // There's no fork on Windows, we should be safe calling this anytime.
  common_init_finish(g_ceph_context);
  global_init_chdir(g_ceph_context);

  return cct;
}

static int do_map(Config *cfg)
{
  int r;

  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx io_ctx;
  librbd::Image image;
  librbd::image_info_t info;
  HANDLE parent_pipe_handle = INVALID_HANDLE_VALUE;
  int err = 0;

  if (g_conf()->daemonize && cfg->parent_pipe.empty()) {
    return send_map_request(get_cli_args());
  }

  dout(0) << "Mapping RBD image: " << cfg->devpath << dendl;

  r = rados.init_with_context(g_ceph_context);
  if (r < 0) {
    derr << "rbd-wnbd: couldn't initialize rados: " << cpp_strerror(r)
         << dendl;
    goto close_ret;
  }

  r = rados.connect();
  if (r < 0) {
    derr << "rbd-wnbd: couldn't connect to rados: " << cpp_strerror(r)
         << dendl;
    goto close_ret;
  }

  r = rados.ioctx_create(cfg->poolname.c_str(), io_ctx);
  if (r < 0) {
    derr << "rbd-wnbd: couldn't create IO context: " << cpp_strerror(r)
         << dendl;
    goto close_ret;
  }

  io_ctx.set_namespace(cfg->nsname);

  r = rbd.open(io_ctx, image, cfg->imgname.c_str());
  if (r < 0) {
    derr << "rbd-wnbd: couldn't open rbd image: " << cpp_strerror(r)
         << dendl;
    goto close_ret;
  }

  if (cfg->exclusive) {
    r = image.lock_acquire(RBD_LOCK_MODE_EXCLUSIVE);
    if (r < 0) {
      derr << "rbd-wnbd: failed to acquire exclusive lock: " << cpp_strerror(r)
           << dendl;
      goto close_ret;
    }
  }

  if (!cfg->snapname.empty()) {
    r = image.snap_set(cfg->snapname.c_str());
    if (r < 0) {
      derr << "rbd-wnbd: couldn't use snapshot: " << cpp_strerror(r)
         << dendl;
      goto close_ret;
    }
  }

  r = image.stat(info, sizeof(info));
  if (r < 0)
    goto close_ret;

  if (info.size > _UI64_MAX) {
    r = -EFBIG;
    derr << "rbd-wnbd: image is too large (" << byte_u_t(info.size)
         << ", max is " << byte_u_t(_UI64_MAX) << ")" << dendl;
    goto close_ret;
  }

  // We're storing mapping details in the registry even for non-persistent
  // mappings. This allows us to easily retrieve mapping details such
  // as the rbd pool or admin socket path.
  // We're cleaning up the registry entry when the non-persistent mapping
  // gets disconnected or when the ceph service restarts.
  r = save_config_to_registry(cfg);
  if (r < 0)
    goto close_ret;

  handler = new WnbdHandler(image, cfg->devpath,
                            info.size / RBD_WNBD_BLKSIZE,
                            RBD_WNBD_BLKSIZE,
                            !cfg->snapname.empty() || cfg->readonly,
                            g_conf().get_val<bool>("rbd_cache"),
                            cfg->io_req_workers,
                            cfg->io_reply_workers);
  r = handler->start();
  if (r) {
    r = r == ERROR_ALREADY_EXISTS ? -EEXIST : -EINVAL;
    goto close_ret;
  }

  // We're informing the parent processes that the initialization
  // was successful.
  if (!cfg->parent_pipe.empty()) {
    parent_pipe_handle = CreateFile(
      cfg->parent_pipe.c_str(), GENERIC_WRITE, 0, NULL,
      OPEN_EXISTING, 0, NULL);
    if (parent_pipe_handle == INVALID_HANDLE_VALUE) {
      derr << "Could not open parent pipe: " << win32_strerror(err) << dendl;
    } else if (!WriteFile(parent_pipe_handle, "a", 1, NULL, NULL)) {
      // TODO: consider exiting in this case. The parent didn't wait for us,
      // maybe it was killed after a timeout.
      err = GetLastError();
      derr << "Failed to communicate with the parent: "
           << win32_strerror(err) << dendl;
    } else {
      dout(5) << __func__ << ": submitted parent notification." << dendl;
    }

    if (parent_pipe_handle != INVALID_HANDLE_VALUE)
      CloseHandle(parent_pipe_handle);

    global_init_postfork_finish(g_ceph_context);
  }

  handler->wait();
  handler->shutdown();

  // The registry record shouldn't be removed for (already) running mappings.
  if (!cfg->persistent) {
    dout(5) << __func__ << ": cleaning up non-persistent mapping: "
            << cfg->devpath << dendl;
    r = remove_config_from_registry(cfg);
    if (r) {
      derr << __func__ << ": could not clean up non-persistent mapping: "
           << cfg->devpath << dendl;
    }
  }

close_ret:
  std::unique_lock l{shutdown_lock};

  image.close();
  io_ctx.close();
  rados.shutdown();
  if (handler) {
    delete handler;
    handler = nullptr;
  }

  return r;
}

static int do_unmap(Config *cfg, bool unregister)
{
  WNBD_REMOVE_OPTIONS remove_options = {0};
  remove_options.Flags.HardRemove = cfg->hard_disconnect;
  remove_options.Flags.HardRemoveFallback = cfg->hard_disconnect_fallback;
  remove_options.SoftRemoveTimeoutMs = cfg->soft_disconnect_timeout * 1000;
  remove_options.SoftRemoveRetryIntervalMs = SOFT_REMOVE_RETRY_INTERVAL * 1000;

  int err = WnbdRemoveEx(cfg->devpath.c_str(), &remove_options);
  if (err && err != ERROR_FILE_NOT_FOUND) {
    return -EINVAL;
  }

  if (unregister) {
    err = remove_config_from_registry(cfg);
    if (err) {
      derr << "rbd-wnbd: failed to unregister device: "
           << cfg->devpath << ". Error: " << err << dendl;
      return -EINVAL;
    }
  }
  return 0;
}

static int parse_imgpath(const std::string &imgpath, Config *cfg,
                         std::ostream *err_msg)
{
  std::regex pattern("^(?:([^/]+)/(?:([^/@]+)/)?)?([^@]+)(?:@([^/@]+))?$");
  std::smatch match;
  if (!std::regex_match(imgpath, match, pattern)) {
    derr << "rbd-wnbd: invalid spec '" << imgpath << "'" << dendl;
    return -EINVAL;
  }

  if (match[1].matched) {
    cfg->poolname = match[1];
  }

  if (match[2].matched) {
    cfg->nsname = match[2];
  }

  cfg->imgname = match[3];

  if (match[4].matched)
    cfg->snapname = match[4];

  return 0;
}

static int do_list_mapped_devices(const std::string &format, bool pretty_format)
{
  std::unique_ptr<ceph::Formatter> f;
  TextTable tbl;

  if (format == "json") {
    f.reset(new JSONFormatter(pretty_format));
  } else if (format == "xml") {
    f.reset(new XMLFormatter(pretty_format));
  } else if (!format.empty() && format != "plain") {
    derr << "rbd-wnbd: invalid output format: " << format << dendl;
    return -EINVAL;
  }

  if (f) {
    f->open_array_section("devices");
  } else {
    tbl.define_column("id", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("pool", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("namespace", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("image", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("snap", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("device", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("disk_number", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("status", TextTable::LEFT, TextTable::LEFT);
  }

  Config cfg;
  WNBDDiskIterator wnbd_disk_iterator;

  while (wnbd_disk_iterator.get(&cfg)) {
    const char* status = cfg.active ?
      WNBD_STATUS_ACTIVE : WNBD_STATUS_INACTIVE;

    if (f) {
      f->open_object_section("device");
      f->dump_int("id", cfg.pid ? cfg.pid : -1);
      f->dump_string("device", cfg.devpath);
      f->dump_string("pool", cfg.poolname);
      f->dump_string("namespace", cfg.nsname);
      f->dump_string("image", cfg.imgname);
      f->dump_string("snap", cfg.snapname);
      f->dump_int("disk_number", cfg.disk_number ? cfg.disk_number : -1);
      f->dump_string("status", status);
      f->close_section();
    } else {
      if (cfg.snapname.empty()) {
          cfg.snapname = "-";
      }
      tbl << (cfg.pid ? cfg.pid : -1) << cfg.poolname << cfg.nsname
          << cfg.imgname << cfg.snapname << cfg.devpath
          << cfg.disk_number << status << TextTable::endrow;
    }
  }
  int error = wnbd_disk_iterator.get_error();
  if (error) {
    derr << "Could not get disk list: " << error << dendl;
    return -error;
  }

  if (f) {
    f->close_section();
    f->flush(std::cout);
  } else {
    std::cout << tbl;
  }

  return 0;
}

static int do_show_mapped_device(std::string format, bool pretty_format,
                                 std::string devpath)
{
  std::unique_ptr<ceph::Formatter> f;
  TextTable tbl;

  if (format.empty() || format == "plain") {
    format = "json";
    pretty_format = true;
  }
  if (format == "json") {
    f.reset(new JSONFormatter(pretty_format));
  } else if (format == "xml") {
    f.reset(new XMLFormatter(pretty_format));
  } else {
    derr << "rbd-wnbd: invalid output format: " << format << dendl;
    return -EINVAL;
  }

  Config cfg;
  int error = load_mapping_config_from_registry(devpath, &cfg);
  if (error) {
    derr << "Could not load registry disk info for: "
         << devpath << ". Error: " << error << dendl;
    return error;
  }

  WNBD_CONNECTION_INFO conn_info = { 0 };
  // If the device is currently disconnected but there is a persistent
  // mapping record, we'll show that.
  DWORD ret = WnbdShow(devpath.c_str(), &conn_info);
  if (ret && ret != ERROR_FILE_NOT_FOUND) {
    return -EINVAL;
  }

  auto conn_props = conn_info.Properties;
  cfg.active = conn_info.DiskNumber > 0 && is_process_running(conn_props.Pid);
  f->open_object_section("device");
  f->dump_int("id", conn_props.Pid ? conn_props.Pid : -1);
  f->dump_string("device", cfg.devpath);
  f->dump_string("pool", cfg.poolname);
  f->dump_string("namespace", cfg.nsname);
  f->dump_string("image", cfg.imgname);
  f->dump_string("snap", cfg.snapname);
  f->dump_int("persistent", cfg.persistent);
  f->dump_int("disk_number", conn_info.DiskNumber ? conn_info.DiskNumber : -1);
  f->dump_string("status", cfg.active ? WNBD_STATUS_ACTIVE : WNBD_STATUS_INACTIVE);
  f->dump_string("pnp_device_id", to_string(conn_info.PNPDeviceID));
  f->dump_int("readonly", conn_props.Flags.ReadOnly);
  f->dump_int("block_size", conn_props.BlockSize);
  f->dump_int("block_count", conn_props.BlockCount);
  f->dump_int("flush_enabled", conn_props.Flags.FlushSupported);
  f->close_section();
  f->flush(std::cout);

  return 0;
}

static int do_stats(std::string search_devpath)
{
  Config cfg;
  WNBDDiskIterator wnbd_disk_iterator;

  while (wnbd_disk_iterator.get(&cfg)) {
    if (cfg.devpath != search_devpath)
      continue;

    AdminSocketClient client = AdminSocketClient(cfg.admin_sock_path);
    std::string output;
    std::string result = client.do_request("{\"prefix\":\"wnbd stats\"}",
                                           &output);
    if (!result.empty()) {
      std::cerr << "Admin socket error: " << result << std::endl;
      return -EINVAL;
    }

    std::cout << output << std::endl;
    return 0;
  }
  int error = wnbd_disk_iterator.get_error();
  if (!error) {
    error = ENOENT;
  }

  derr << "Could not find the specified disk." << dendl;
  return -error;
}

static int parse_args(std::vector<const char*>& args,
                      std::ostream *err_msg,
                      Command *command, Config *cfg)
{
  std::string conf_file_list;
  std::string cluster;
  CephInitParameters iparams = ceph_argparse_early_args(
          args, CEPH_ENTITY_TYPE_CLIENT, &cluster, &conf_file_list);

  ConfigProxy config{false};
  config->name = iparams.name;
  config->cluster = cluster;

  if (!conf_file_list.empty()) {
    config.parse_config_files(conf_file_list.c_str(), nullptr, 0);
  } else {
    config.parse_config_files(nullptr, nullptr, 0);
  }
  config.parse_env(CEPH_ENTITY_TYPE_CLIENT);
  config.parse_argv(args);
  cfg->poolname = config.get_val<std::string>("rbd_default_pool");

  std::vector<const char*>::iterator i;
  std::ostringstream err;

  // TODO: consider using boost::program_options like Device.cc does.
  // This should simplify argument parsing. Also, some arguments must be tied
  // to specific commands, for example the disconnect timeout. Luckily,
  // this is enforced by the "rbd device" wrapper.
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      return HELP_INFO;
    } else if (ceph_argparse_flag(args, i, "-v", "--version", (char*)NULL)) {
      return VERSION_INFO;
    } else if (ceph_argparse_witharg(args, i, &cfg->devpath, "--device", (char *)NULL)) {
    } else if (ceph_argparse_witharg(args, i, &cfg->format, err, "--format",
                                     (char *)NULL)) {
    } else if (ceph_argparse_flag(args, i, "--read-only", (char *)NULL)) {
      cfg->readonly = true;
    } else if (ceph_argparse_flag(args, i, "--exclusive", (char *)NULL)) {
      cfg->exclusive = true;
    } else if (ceph_argparse_flag(args, i, "--non-persistent", (char *)NULL)) {
      cfg->persistent = false;
    } else if (ceph_argparse_flag(args, i, "--pretty-format", (char *)NULL)) {
      cfg->pretty_format = true;
    } else if (ceph_argparse_flag(args, i, "--remap-failure-fatal", (char *)NULL)) {
      cfg->remap_failure_fatal = true;
    } else if (ceph_argparse_witharg(args, i, &cfg->parent_pipe, err,
                                     "--pipe-name", (char *)NULL)) {
      if (!err.str().empty()) {
        *err_msg << "rbd-wnbd: " << err.str();
        return -EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, (int*)&cfg->wnbd_log_level,
                                     err, "--wnbd-log-level", (char *)NULL)) {
      if (!err.str().empty()) {
        *err_msg << "rbd-wnbd: " << err.str();
        return -EINVAL;
      }
      if (cfg->wnbd_log_level < 0) {
        *err_msg << "rbd-wnbd: Invalid argument for wnbd-log-level";
        return -EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, (int*)&cfg->io_req_workers,
                                     err, "--io-req-workers", (char *)NULL)) {
      if (!err.str().empty()) {
        *err_msg << "rbd-wnbd: " << err.str();
        return -EINVAL;
      }
      if (cfg->io_req_workers <= 0) {
        *err_msg << "rbd-wnbd: Invalid argument for io-req-workers";
        return -EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, (int*)&cfg->io_reply_workers,
                                     err, "--io-reply-workers", (char *)NULL)) {
      if (!err.str().empty()) {
        *err_msg << "rbd-wnbd: " << err.str();
        return -EINVAL;
      }
      if (cfg->io_reply_workers <= 0) {
        *err_msg << "rbd-wnbd: Invalid argument for io-reply-workers";
        return -EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, (int*)&cfg->service_thread_count,
                                     err, "--service-thread-count", (char *)NULL)) {
      if (!err.str().empty()) {
        *err_msg << "rbd-wnbd: " << err.str();
        return -EINVAL;
      }
      if (cfg->service_thread_count <= 0) {
        *err_msg << "rbd-wnbd: Invalid argument for service-thread-count";
        return -EINVAL;
      }
    } else if (ceph_argparse_flag(args, i, "--hard-disconnect", (char *)NULL)) {
      cfg->hard_disconnect = true;
    } else if (ceph_argparse_flag(args, i,
                                  "--no-hard-disconnect-fallback", (char *)NULL)) {
      cfg->hard_disconnect_fallback = false;
    } else if (ceph_argparse_witharg(args, i,
                                     (int*)&cfg->soft_disconnect_timeout,
                                     err, "--soft-disconnect-timeout",
                                     (char *)NULL)) {
      if (!err.str().empty()) {
        *err_msg << "rbd-wnbd: " << err.str();
        return -EINVAL;
      }
      if (cfg->soft_disconnect_timeout < 0) {
        *err_msg << "rbd-wnbd: Invalid argument for soft-disconnect-timeout";
        return -EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i,
                                     (int*)&cfg->service_start_timeout,
                                     err, "--start-timeout",
                                     (char *)NULL)) {
      if (!err.str().empty()) {
        *err_msg << "rbd-wnbd: " << err.str();
        return -EINVAL;
      }
      if (cfg->service_start_timeout <= 0) {
        *err_msg << "rbd-wnbd: Invalid argument for start-timeout";
        return -EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i,
                                     (int*)&cfg->image_map_timeout,
                                     err, "--map-timeout",
                                     (char *)NULL)) {
      if (!err.str().empty()) {
        *err_msg << "rbd-wnbd: " << err.str();
        return -EINVAL;
      }
      if (cfg->image_map_timeout <= 0) {
        *err_msg << "rbd-wnbd: Invalid argument for map-timeout";
        return -EINVAL;
      }
    } else {
      ++i;
    }
  }

  Command cmd = None;
  if (args.begin() != args.end()) {
    if (strcmp(*args.begin(), "map") == 0) {
      cmd = Connect;
    } else if (strcmp(*args.begin(), "unmap") == 0) {
      cmd = Disconnect;
    } else if (strcmp(*args.begin(), "list") == 0) {
      cmd = List;
    } else if (strcmp(*args.begin(), "show") == 0) {
      cmd = Show;
    } else if (strcmp(*args.begin(), "service") == 0) {
      cmd = Service;
    } else if (strcmp(*args.begin(), "stats") == 0) {
      cmd = Stats;
    } else if (strcmp(*args.begin(), "help") == 0) {
      return HELP_INFO;
    } else {
      *err_msg << "rbd-wnbd: unknown command: " <<  *args.begin();
      return -EINVAL;
    }
    args.erase(args.begin());
  }

  if (cmd == None) {
    *err_msg << "rbd-wnbd: must specify command";
    return -EINVAL;
  }

  switch (cmd) {
    case Connect:
    case Disconnect:
    case Show:
    case Stats:
      if (args.begin() == args.end()) {
        *err_msg << "rbd-wnbd: must specify wnbd device or image-or-snap-spec";
        return -EINVAL;
      }
      if (parse_imgpath(*args.begin(), cfg, err_msg) < 0) {
        return -EINVAL;
      }
      args.erase(args.begin());
      break;
    default:
      //shut up gcc;
      break;
  }

  if (args.begin() != args.end()) {
    *err_msg << "rbd-wnbd: unknown args: " << *args.begin();
    return -EINVAL;
  }

  *command = cmd;
  return 0;
}

static int rbd_wnbd(int argc, const char *argv[])
{
  Config cfg;
  auto args = argv_to_vec(argc, argv);

  // Avoid using dout before calling "do_global_init"
  if (args.empty()) {
    std::cout << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }

  std::ostringstream err_msg;
  int r = parse_args(args, &err_msg, &cmd, &cfg);
  if (r == HELP_INFO) {
    usage();
    return 0;
  } else if (r == VERSION_INFO) {
    std::cout << pretty_version_to_str() << std::endl;
    return 0;
  } else if (r < 0) {
    std::cout << err_msg.str() << std::endl;
    return r;
  }

  auto cct = do_global_init(argc, argv, &cfg);

  WnbdSetLogger(WnbdHandler::LogMessage);
  WnbdSetLogLevel(cfg.wnbd_log_level);

  switch (cmd) {
    case Connect:
      if (construct_devpath_if_missing(&cfg)) {
        return -EINVAL;
      }
      r = do_map(&cfg);
      if (r < 0)
        return r;
      break;
    case Disconnect:
      if (construct_devpath_if_missing(&cfg)) {
        return -EINVAL;
      }
      r = do_unmap(&cfg, true);
      if (r < 0)
        return r;
      break;
    case List:
      r = do_list_mapped_devices(cfg.format, cfg.pretty_format);
      if (r < 0)
        return r;
      break;
    case Show:
      if (construct_devpath_if_missing(&cfg)) {
        return r;
      }
      r = do_show_mapped_device(cfg.format, cfg.pretty_format, cfg.devpath);
      if (r < 0)
        return r;
      break;
    case Service:
    {
      RBDService service(cfg.hard_disconnect, cfg.soft_disconnect_timeout,
                         cfg.service_thread_count,
                         cfg.service_start_timeout,
                         cfg.image_map_timeout,
                         cfg.remap_failure_fatal);
      // This call will block until the service stops.
      r = RBDService::initialize(&service);
      if (r < 0)
        return r;
      break;
    }
    case Stats:
      if (construct_devpath_if_missing(&cfg)) {
        return -EINVAL;
      }
      return do_stats(cfg.devpath);
    default:
      usage();
      break;
  }

  return 0;
}

int main(int argc, const char *argv[])
{
  SetConsoleCtrlHandler(console_handler_routine, true);
  // Avoid the Windows Error Reporting dialog.
  SetErrorMode(GetErrorMode() | SEM_NOGPFAULTERRORBOX);
  int r = rbd_wnbd(argc, argv);
  if (r < 0) {
    return r;
  }
  return 0;
}
