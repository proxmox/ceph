// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/errno.h"

#include "rgw_zone.h"
#include "rgw_realm_watcher.h"
#include "rgw_meta_sync_status.h"
#include "rgw_sync.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw_zone_defaults {

std::string zone_info_oid_prefix = "zone_info.";
std::string zone_names_oid_prefix = "zone_names.";
std::string region_info_oid_prefix = "region_info.";
std::string realm_names_oid_prefix = "realms_names.";
std::string zone_group_info_oid_prefix = "zonegroup_info.";
std::string realm_info_oid_prefix = "realms.";
std::string default_region_info_oid = "default.region";
std::string default_zone_group_info_oid = "default.zonegroup";
std::string period_info_oid_prefix = "periods.";
std::string period_latest_epoch_info_oid = ".latest_epoch";
std::string region_map_oid = "region_map";
std::string default_realm_info_oid = "default.realm";
std::string default_zonegroup_name = "default";
std::string default_zone_name = "default";
std::string zonegroup_names_oid_prefix = "zonegroups_names.";
std::string RGW_DEFAULT_ZONE_ROOT_POOL = "rgw.root";
std::string RGW_DEFAULT_ZONEGROUP_ROOT_POOL = "rgw.root";
std::string RGW_DEFAULT_REALM_ROOT_POOL = "rgw.root";
std::string RGW_DEFAULT_PERIOD_ROOT_POOL = "rgw.root";
std::string default_bucket_index_pool_suffix = "rgw.buckets.index";
std::string default_storage_extra_pool_suffix = "rgw.buckets.non-ec";
std::string avail_pools = ".pools.avail";
std::string default_storage_pool_suffix = "rgw.buckets.data";

}

using namespace rgw_zone_defaults;

#define FIRST_EPOCH 1

void RGWDefaultZoneGroupInfo::dump(Formatter *f) const {
  encode_json("default_zonegroup", default_zonegroup, f);
}

void RGWDefaultZoneGroupInfo::decode_json(JSONObj *obj) {

  JSONDecoder::decode_json("default_zonegroup", default_zonegroup, obj);
  /* backward compatability with region */
  if (default_zonegroup.empty()) {
    JSONDecoder::decode_json("default_region", default_zonegroup, obj);
  }
}

rgw_pool RGWZoneGroup::get_pool(CephContext *cct_) const
{
  if (cct_->_conf->rgw_zonegroup_root_pool.empty()) {
    return rgw_pool(RGW_DEFAULT_ZONEGROUP_ROOT_POOL);
  }

  return rgw_pool(cct_->_conf->rgw_zonegroup_root_pool);
}

int RGWZoneGroup::create_default(const DoutPrefixProvider *dpp, optional_yield y, bool old_format)
{
  name = default_zonegroup_name;
  api_name = default_zonegroup_name;
  is_master = true;

  RGWZoneGroupPlacementTarget placement_target;
  placement_target.name = "default-placement";
  placement_targets[placement_target.name] = placement_target;
  default_placement.name = "default-placement";

  RGWZoneParams zone_params(default_zone_name);

  int r = zone_params.init(dpp, cct, sysobj_svc, y, false);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "create_default: error initializing zone params: " << cpp_strerror(-r) << dendl;
    return r;
  }

  r = zone_params.create_default(dpp, y);
  if (r < 0 && r != -EEXIST) {
    ldpp_dout(dpp, 0) << "create_default: error in create_default  zone params: " << cpp_strerror(-r) << dendl;
    return r;
  } else if (r == -EEXIST) {
    ldpp_dout(dpp, 10) << "zone_params::create_default() returned -EEXIST, we raced with another default zone_params creation" << dendl;
    zone_params.clear_id();
    r = zone_params.init(dpp, cct, sysobj_svc, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "create_default: error in init existing zone params: " << cpp_strerror(-r) << dendl;
      return r;
    }
    ldpp_dout(dpp, 20) << "zone_params::create_default() " << zone_params.get_name() << " id " << zone_params.get_id()
		   << dendl;
  }
  
  RGWZone& default_zone = zones[zone_params.get_id()];
  default_zone.name = zone_params.get_name();
  default_zone.id = zone_params.get_id();
  master_zone = default_zone.id;
  
  r = create(dpp, y);
  if (r < 0 && r != -EEXIST) {
    ldpp_dout(dpp, 0) << "error storing zone group info: " << cpp_strerror(-r) << dendl;
    return r;
  }

  if (r == -EEXIST) {
    ldpp_dout(dpp, 10) << "create_default() returned -EEXIST, we raced with another zonegroup creation" << dendl;
    id.clear();
    r = init(dpp, cct, sysobj_svc, y);
    if (r < 0) {
      return r;
    }
  }

  if (old_format) {
    name = id;
  }

  post_process_params(dpp, y);

  return 0;
}

const string RGWZoneGroup::get_default_oid(bool old_region_format) const
{
  if (old_region_format) {
    if (cct->_conf->rgw_default_region_info_oid.empty()) {
      return default_region_info_oid;
    }
    return cct->_conf->rgw_default_region_info_oid;
  }

  string default_oid = cct->_conf->rgw_default_zonegroup_info_oid;

  if (cct->_conf->rgw_default_zonegroup_info_oid.empty()) {
    default_oid = default_zone_group_info_oid;
  }

  default_oid += "." + realm_id;

  return default_oid;
}

const string& RGWZoneGroup::get_info_oid_prefix(bool old_region_format) const
{
  if (old_region_format) {
    return region_info_oid_prefix;
  }
  return zone_group_info_oid_prefix;
}

const string& RGWZoneGroup::get_names_oid_prefix() const
{
  return zonegroup_names_oid_prefix;
}

const string& RGWZoneGroup::get_predefined_name(CephContext *cct) const {
  return cct->_conf->rgw_zonegroup;
}

int RGWZoneGroup::equals(const string& other_zonegroup) const
{
  if (is_master && other_zonegroup.empty())
    return true;

  return (id  == other_zonegroup);
}

int RGWZoneGroup::add_zone(const DoutPrefixProvider *dpp, 
                           const RGWZoneParams& zone_params, bool *is_master, bool *read_only,
                           const list<string>& endpoints, const string *ptier_type,
                           bool *psync_from_all, list<string>& sync_from, list<string>& sync_from_rm,
                           string *predirect_zone, std::optional<int> bucket_index_max_shards,
                           RGWSyncModulesManager *sync_mgr,
			   optional_yield y)
{
  auto& zone_id = zone_params.get_id();
  auto& zone_name = zone_params.get_name();

  // check for duplicate zone name on insert
  if (!zones.count(zone_id)) {
    for (const auto& zone : zones) {
      if (zone.second.name == zone_name) {
        ldpp_dout(dpp, 0) << "ERROR: found existing zone name " << zone_name
            << " (" << zone.first << ") in zonegroup " << get_name() << dendl;
        return -EEXIST;
      }
    }
  }

  if (is_master) {
    if (*is_master) {
      if (!master_zone.empty() && master_zone != zone_id) {
        ldpp_dout(dpp, 0) << "NOTICE: overriding master zone: " << master_zone << dendl;
      }
      master_zone = zone_id;
    } else if (master_zone == zone_id) {
      master_zone.clear();
    }
  }

  RGWZone& zone = zones[zone_id];
  zone.name = zone_name;
  zone.id = zone_id;
  if (!endpoints.empty()) {
    zone.endpoints = endpoints;
  }
  if (read_only) {
    zone.read_only = *read_only;
  }
  if (ptier_type) {
    zone.tier_type = *ptier_type;
    if (!sync_mgr->get_module(*ptier_type, nullptr)) {
      ldpp_dout(dpp, 0) << "ERROR: could not found sync module: " << *ptier_type 
                    << ",  valid sync modules: " 
                    << sync_mgr->get_registered_module_names()
                    << dendl;
      return -ENOENT;
    }
  }

  if (psync_from_all) {
    zone.sync_from_all = *psync_from_all;
  }

  if (predirect_zone) {
    zone.redirect_zone = *predirect_zone;
  }

  if (bucket_index_max_shards) {
    zone.bucket_index_max_shards = *bucket_index_max_shards;
  }

  for (auto add : sync_from) {
    zone.sync_from.insert(add);
  }

  for (auto rm : sync_from_rm) {
    zone.sync_from.erase(rm);
  }

  post_process_params(dpp, y);

  return update(dpp,y);
}


int RGWZoneGroup::rename_zone(const DoutPrefixProvider *dpp, 
                              const RGWZoneParams& zone_params,
			      optional_yield y)
{
  RGWZone& zone = zones[zone_params.get_id()];
  zone.name = zone_params.get_name();

  return update(dpp, y);
}

void RGWZoneGroup::post_process_params(const DoutPrefixProvider *dpp, optional_yield y)
{
  bool log_data = zones.size() > 1;

  if (master_zone.empty()) {
    auto iter = zones.begin();
    if (iter != zones.end()) {
      master_zone = iter->first;
    }
  }
  
  for (auto& item : zones) {
    RGWZone& zone = item.second;
    zone.log_data = log_data;

    RGWZoneParams zone_params(zone.id, zone.name);
    int ret = zone_params.init(dpp, cct, sysobj_svc, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "WARNING: could not read zone params for zone id=" << zone.id << " name=" << zone.name << dendl;
      continue;
    }

    for (auto& pitem : zone_params.placement_pools) {
      const string& placement_name = pitem.first;
      if (placement_targets.find(placement_name) == placement_targets.end()) {
        RGWZoneGroupPlacementTarget placement_target;
        placement_target.name = placement_name;
        placement_targets[placement_name] = placement_target;
      }
    }
  }

  if (default_placement.empty() && !placement_targets.empty()) {
    default_placement.init(placement_targets.begin()->first, RGW_STORAGE_CLASS_STANDARD);
  }
}

int RGWZoneGroup::remove_zone(const DoutPrefixProvider *dpp, const std::string& zone_id, optional_yield y)
{
  auto iter = zones.find(zone_id);
  if (iter == zones.end()) {
    ldpp_dout(dpp, 0) << "zone id " << zone_id << " is not a part of zonegroup "
        << name << dendl;
    return -ENOENT;
  }

  zones.erase(iter);

  post_process_params(dpp, y);

  return update(dpp, y);
}

int RGWZoneGroup::read_default_id(const DoutPrefixProvider *dpp, string& default_id, optional_yield y,
				  bool old_format)
{
  if (realm_id.empty()) {
    /* try using default realm */
    RGWRealm realm;
    int ret = realm.init(dpp, cct, sysobj_svc, y);
    // no default realm exist
    if (ret < 0) {
      return read_id(dpp, default_zonegroup_name, default_id, y);
    }
    realm_id = realm.get_id();
  }

  return RGWSystemMetaObj::read_default_id(dpp, default_id, y, old_format);
}

int RGWZoneGroup::set_as_default(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive)
{
  if (realm_id.empty()) {
    /* try using default realm */
    RGWRealm realm;
    int ret = realm.init(dpp, cct, sysobj_svc, y);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "could not read realm id: " << cpp_strerror(-ret) << dendl;
      return -EINVAL;
    }
    realm_id = realm.get_id();
  }

  return RGWSystemMetaObj::set_as_default(dpp, y, exclusive);
}

void RGWSystemMetaObj::reinit_instance(CephContext *_cct, RGWSI_SysObj *_sysobj_svc)
{
  cct = _cct;
  sysobj_svc = _sysobj_svc;
  zone_svc = _sysobj_svc->get_zone_svc();
}

int RGWSystemMetaObj::init(const DoutPrefixProvider *dpp, CephContext *_cct, RGWSI_SysObj *_sysobj_svc,
			   optional_yield y,
			   bool setup_obj, bool old_format)
{
  reinit_instance(_cct, _sysobj_svc);

  if (!setup_obj)
    return 0;

  if (old_format && id.empty()) {
    id = name;
  }

  if (id.empty()) {
    int r;
    if (name.empty()) {
      name = get_predefined_name(cct);
    }
    if (name.empty()) {
      r = use_default(dpp, y, old_format);
      if (r < 0) {
	return r;
      }
    } else if (!old_format) {
      r = read_id(dpp, name, id, y);
      if (r < 0) {
        if (r != -ENOENT) {
          ldpp_dout(dpp, 0) << "error in read_id for object name: " << name << " : " << cpp_strerror(-r) << dendl;
        }
        return r;
      }
    }
  }

  return read_info(dpp, id, y, old_format);
}

int RGWSystemMetaObj::read_default(const DoutPrefixProvider *dpp, 
                                   RGWDefaultSystemMetaObjInfo& default_info,
				   const string& oid, optional_yield y)
{
  using ceph::decode;
  auto pool = get_pool(cct);
  bufferlist bl;

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));
  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0)
    return ret;

  try {
    auto iter = bl.cbegin();
    decode(default_info, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "error decoding data from " << pool << ":" << oid << dendl;
    return -EIO;
  }

  return 0;
}

int RGWSystemMetaObj::read_default_id(const DoutPrefixProvider *dpp, string& default_id, optional_yield y,
				      bool old_format)
{
  RGWDefaultSystemMetaObjInfo default_info;

  int ret = read_default(dpp, default_info, get_default_oid(old_format), y);
  if (ret < 0) {
    return ret;
  }

  default_id = default_info.default_id;

  return 0;
}

int RGWSystemMetaObj::use_default(const DoutPrefixProvider *dpp, optional_yield y, bool old_format)
{
  return read_default_id(dpp, id, y, old_format);
}

int RGWSystemMetaObj::set_as_default(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive)
{
  using ceph::encode;
  string oid  = get_default_oid();

  rgw_pool pool(get_pool(cct));
  bufferlist bl;

  RGWDefaultSystemMetaObjInfo default_info;
  default_info.default_id = id;

  encode(default_info, bl);

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));
  int ret = sysobj.wop()
                  .set_exclusive(exclusive)
                  .write(dpp, bl, y);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWSystemMetaObj::read_id(const DoutPrefixProvider *dpp, const string& obj_name, string& object_id,
			      optional_yield y)
{
  using ceph::decode;
  rgw_pool pool(get_pool(cct));
  bufferlist bl;

  string oid = get_names_oid_prefix() + obj_name;

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));
  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0) {
    return ret;
  }

  RGWNameToId nameToId;
  try {
    auto iter = bl.cbegin();
    decode(nameToId, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode obj from " << pool << ":" << oid << dendl;
    return -EIO;
  }
  object_id = nameToId.obj_id;
  return 0;
}

int RGWSystemMetaObj::delete_obj(const DoutPrefixProvider *dpp, optional_yield y, bool old_format)
{
  rgw_pool pool(get_pool(cct));

  auto obj_ctx = sysobj_svc->init_obj_ctx();

  /* check to see if obj is the default */
  RGWDefaultSystemMetaObjInfo default_info;
  int ret = read_default(dpp, default_info, get_default_oid(old_format), y);
  if (ret < 0 && ret != -ENOENT)
    return ret;
  if (default_info.default_id == id || (old_format && default_info.default_id == name)) {
    string oid = get_default_oid(old_format);
    rgw_raw_obj default_named_obj(pool, oid);
    auto sysobj = sysobj_svc->get_obj(obj_ctx, default_named_obj);
    ret = sysobj.wop().remove(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "Error delete default obj name  " << name << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }
  if (!old_format) {
    string oid  = get_names_oid_prefix() + name;
    rgw_raw_obj object_name(pool, oid);
    auto sysobj = sysobj_svc->get_obj(obj_ctx, object_name);
    ret = sysobj.wop().remove(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "Error delete obj name  " << name << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  string oid = get_info_oid_prefix(old_format);
  if (old_format) {
    oid += name;
  } else {
    oid += id;
  }

  rgw_raw_obj object_id(pool, oid);
  auto sysobj = sysobj_svc->get_obj(obj_ctx, object_id);
  ret = sysobj.wop().remove(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Error delete object id " << id << ": " << cpp_strerror(-ret) << dendl;
  }

  return ret;
}

int RGWSystemMetaObj::store_name(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  rgw_pool pool(get_pool(cct));
  string oid = get_names_oid_prefix() + name;

  RGWNameToId nameToId;
  nameToId.obj_id = id;

  bufferlist bl;
  using ceph::encode;
  encode(nameToId, bl);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));
  return sysobj.wop()
               .set_exclusive(exclusive)
               .write(dpp, bl, y);
}

int RGWSystemMetaObj::rename(const DoutPrefixProvider *dpp, const string& new_name, optional_yield y)
{
  string new_id;
  int ret = read_id(dpp, new_name, new_id, y);
  if (!ret) {
    return -EEXIST;
  }
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "Error read_id " << new_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  string old_name = name;
  name = new_name;
  ret = update(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Error storing new obj info " << new_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  ret = store_name(dpp, true, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Error storing new name " << new_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  /* delete old name */
  rgw_pool pool(get_pool(cct));
  string oid = get_names_oid_prefix() + old_name;
  rgw_raw_obj old_name_obj(pool, oid);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, old_name_obj);
  ret = sysobj.wop().remove(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Error delete old obj name  " << old_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return ret;
}

int RGWSystemMetaObj::read_info(const DoutPrefixProvider *dpp, const string& obj_id, optional_yield y,
				bool old_format)
{
  rgw_pool pool(get_pool(cct));

  bufferlist bl;

  string oid = get_info_oid_prefix(old_format) + obj_id;

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, oid});
  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failed reading obj info from " << pool << ":" << oid << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  using ceph::decode;

  try {
    auto iter = bl.cbegin();
    decode(*this, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode obj from " << pool << ":" << oid << dendl;
    return -EIO;
  }

  return 0;
}

int RGWSystemMetaObj::read(const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret = read_id(dpp, name, id, y);
  if (ret < 0) {
    return ret;
  }

  return read_info(dpp, id, y);
}

int RGWSystemMetaObj::create(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive)
{
  int ret;

  /* check to see the name is not used */
  ret = read_id(dpp, name, id, y);
  if (exclusive && ret == 0) {
    ldpp_dout(dpp, 10) << "ERROR: name " << name << " already in use for obj id " << id << dendl;
    return -EEXIST;
  } else if ( ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "failed reading obj id  " << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  if (id.empty()) {
    /* create unique id */
    uuid_d new_uuid;
    char uuid_str[37];
    new_uuid.generate_random();
    new_uuid.print(uuid_str);
    id = uuid_str;
  }

  ret = store_info(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR:  storing info for " << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return store_name(dpp, exclusive, y);
}

int RGWSystemMetaObj::store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  rgw_pool pool(get_pool(cct));

  string oid = get_info_oid_prefix() + id;

  bufferlist bl;
  using ceph::encode;
  encode(*this, bl);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, oid});
  return sysobj.wop()
               .set_exclusive(exclusive)
               .write(dpp, bl, y);
}

int RGWSystemMetaObj::write(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  int ret = store_info(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << __func__ << "(): store_info() returned ret=" << ret << dendl;
    return ret;
  }
  ret = store_name(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << __func__ << "(): store_name() returned ret=" << ret << dendl;
    return ret;
  }
  return 0;
}


const string& RGWRealm::get_predefined_name(CephContext *cct) const {
  return cct->_conf->rgw_realm;
}

int RGWRealm::create(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive)
{
  int ret = RGWSystemMetaObj::create(dpp, y, exclusive);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR creating new realm object " << name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  // create the control object for watch/notify
  ret = create_control(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR creating control for new realm " << name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  RGWPeriod period;
  if (current_period.empty()) {
    /* create new period for the realm */
    ret = period.init(dpp, cct, sysobj_svc, id, y, name, false);
    if (ret < 0 ) {
      return ret;
    }
    ret = period.create(dpp, y, true);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: creating new period for realm " << name << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  } else {
    period = RGWPeriod(current_period, 0);
    int ret = period.init(dpp, cct, sysobj_svc, id, y, name);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to init period " << current_period << dendl;
      return ret;
    }
  }
  ret = set_current_period(dpp, period, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed set current period " << current_period << dendl;
    return ret;
  }
  // try to set as default. may race with another create, so pass exclusive=true
  // so we don't override an existing default
  ret = set_as_default(dpp, y, true);
  if (ret < 0 && ret != -EEXIST) {
    ldpp_dout(dpp, 0) << "WARNING: failed to set realm as default realm, ret=" << ret << dendl;
  }

  return 0;
}

int RGWRealm::delete_obj(const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret = RGWSystemMetaObj::delete_obj(dpp, y);
  if (ret < 0) {
    return ret;
  }
  return delete_control(dpp, y);
}

int RGWRealm::create_control(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  auto pool = rgw_pool{get_pool(cct)};
  auto oid = get_control_oid();
  bufferlist bl;
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, oid});
  return sysobj.wop()
               .set_exclusive(exclusive)
               .write(dpp, bl, y);
}

int RGWRealm::delete_control(const DoutPrefixProvider *dpp, optional_yield y)
{
  auto pool = rgw_pool{get_pool(cct)};
  auto obj = rgw_raw_obj{pool, get_control_oid()};
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, obj);
  return sysobj.wop().remove(dpp, y);
}

rgw_pool RGWRealm::get_pool(CephContext *cct) const
{
  if (cct->_conf->rgw_realm_root_pool.empty()) {
    return rgw_pool(RGW_DEFAULT_REALM_ROOT_POOL);
  }
  return rgw_pool(cct->_conf->rgw_realm_root_pool);
}

const string RGWRealm::get_default_oid(bool old_format) const
{
  if (cct->_conf->rgw_default_realm_info_oid.empty()) {
    return default_realm_info_oid;
  }
  return cct->_conf->rgw_default_realm_info_oid;
}

const string& RGWRealm::get_names_oid_prefix() const
{
  return realm_names_oid_prefix;
}

const string& RGWRealm::get_info_oid_prefix(bool old_format) const
{
  return realm_info_oid_prefix;
}

int RGWRealm::set_current_period(const DoutPrefixProvider *dpp, RGWPeriod& period, optional_yield y)
{
  // update realm epoch to match the period's
  if (epoch > period.get_realm_epoch()) {
    ldpp_dout(dpp, 0) << "ERROR: set_current_period with old realm epoch "
        << period.get_realm_epoch() << ", current epoch=" << epoch << dendl;
    return -EINVAL;
  }
  if (epoch == period.get_realm_epoch() && current_period != period.get_id()) {
    ldpp_dout(dpp, 0) << "ERROR: set_current_period with same realm epoch "
        << period.get_realm_epoch() << ", but different period id "
        << period.get_id() << " != " << current_period << dendl;
    return -EINVAL;
  }

  epoch = period.get_realm_epoch();
  current_period = period.get_id();

  int ret = update(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: period update: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = period.reflect(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: period.reflect(): " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

string RGWRealm::get_control_oid() const
{
  return get_info_oid_prefix() + id + ".control";
}

int RGWRealm::notify_zone(const DoutPrefixProvider *dpp, bufferlist& bl, optional_yield y)
{
  rgw_pool pool{get_pool(cct)};
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, get_control_oid()});
  int ret = sysobj.wn().notify(dpp, bl, 0, nullptr, y);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

int RGWRealm::notify_new_period(const DoutPrefixProvider *dpp, const RGWPeriod& period, optional_yield y)
{
  bufferlist bl;
  using ceph::encode;
  // push the period to dependent zonegroups/zones
  encode(RGWRealmNotify::ZonesNeedPeriod, bl);
  encode(period, bl);
  // reload the gateway with the new period
  encode(RGWRealmNotify::Reload, bl);

  return notify_zone(dpp, bl, y);
}

std::string RGWPeriodConfig::get_oid(const std::string& realm_id)
{
  if (realm_id.empty()) {
    return "period_config.default";
  }
  return "period_config." + realm_id;
}

rgw_pool RGWPeriodConfig::get_pool(CephContext *cct)
{
  const auto& pool_name = cct->_conf->rgw_period_root_pool;
  if (pool_name.empty()) {
    return {RGW_DEFAULT_PERIOD_ROOT_POOL};
  }
  return {pool_name};
}

int RGWPeriodConfig::read(const DoutPrefixProvider *dpp, RGWSI_SysObj *sysobj_svc, const std::string& realm_id,
			  optional_yield y)
{
  const auto& pool = get_pool(sysobj_svc->ctx());
  const auto& oid = get_oid(realm_id);
  bufferlist bl;

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, oid});
  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0) {
    return ret;
  }
  using ceph::decode;
  try {
    auto iter = bl.cbegin();
    decode(*this, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }
  return 0;
}

int RGWPeriodConfig::write(const DoutPrefixProvider *dpp, 
                           RGWSI_SysObj *sysobj_svc,
			   const std::string& realm_id, optional_yield y)
{
  const auto& pool = get_pool(sysobj_svc->ctx());
  const auto& oid = get_oid(realm_id);
  bufferlist bl;
  using ceph::encode;
  encode(*this, bl);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, oid});
  return sysobj.wop()
               .set_exclusive(false)
               .write(dpp, bl, y);
}

int RGWPeriod::init(const DoutPrefixProvider *dpp, CephContext *_cct, RGWSI_SysObj *_sysobj_svc,
		    const string& period_realm_id, optional_yield y,
		    const string& period_realm_name, bool setup_obj)
{
  cct = _cct;
  sysobj_svc = _sysobj_svc;

  realm_id = period_realm_id;
  realm_name = period_realm_name;

  if (!setup_obj)
    return 0;

  return init(dpp, _cct, _sysobj_svc, y, setup_obj);
}


int RGWPeriod::init(const DoutPrefixProvider *dpp, 
                    CephContext *_cct, RGWSI_SysObj *_sysobj_svc,
		    optional_yield y, bool setup_obj)
{
  cct = _cct;
  sysobj_svc = _sysobj_svc;

  if (!setup_obj)
    return 0;

  if (id.empty()) {
    RGWRealm realm(realm_id, realm_name);
    int ret = realm.init(dpp, cct, sysobj_svc, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "RGWPeriod::init failed to init realm " << realm_name  << " id " << realm_id << " : " <<
	cpp_strerror(-ret) << dendl;
      return ret;
    }
    id = realm.get_current_period();
    realm_id = realm.get_id();
  }

  if (!epoch) {
    int ret = use_latest_epoch(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "failed to use_latest_epoch period id " << id << " realm " << realm_name  << " id " << realm_id
	   << " : " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  return read_info(dpp, y);
}


int RGWPeriod::get_zonegroup(RGWZoneGroup& zonegroup,
                             const string& zonegroup_id) const
{
  map<string, RGWZoneGroup>::const_iterator iter;
  if (!zonegroup_id.empty()) {
    iter = period_map.zonegroups.find(zonegroup_id);
  } else {
    iter = period_map.zonegroups.find("default");
  }
  if (iter != period_map.zonegroups.end()) {
    zonegroup = iter->second;
    return 0;
  }

  return -ENOENT;
}

const string& RGWPeriod::get_latest_epoch_oid() const
{
  if (cct->_conf->rgw_period_latest_epoch_info_oid.empty()) {
    return period_latest_epoch_info_oid;
  }
  return cct->_conf->rgw_period_latest_epoch_info_oid;
}

const string& RGWPeriod::get_info_oid_prefix() const
{
  return period_info_oid_prefix;
}

const string RGWPeriod::get_period_oid_prefix() const
{
  return get_info_oid_prefix() + id;
}

const string RGWPeriod::get_period_oid() const
{
  std::ostringstream oss;
  oss << get_period_oid_prefix();
  // skip the epoch for the staging period
  if (id != get_staging_id(realm_id))
    oss << "." << epoch;
  return oss.str();
}

int RGWPeriod::read_latest_epoch(const DoutPrefixProvider *dpp, 
                                 RGWPeriodLatestEpochInfo& info,
				 optional_yield y,
                                 RGWObjVersionTracker *objv)
{
  string oid = get_period_oid_prefix() + get_latest_epoch_oid();

  rgw_pool pool(get_pool(cct));
  bufferlist bl;
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, oid});
  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "error read_lastest_epoch " << pool << ":" << oid << dendl;
    return ret;
  }
  try {
    auto iter = bl.cbegin();
    using ceph::decode;
    decode(info, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "error decoding data from " << pool << ":" << oid << dendl;
    return -EIO;
  }

  return 0;
}

int RGWPeriod::get_latest_epoch(const DoutPrefixProvider *dpp, epoch_t& latest_epoch, optional_yield y)
{
  RGWPeriodLatestEpochInfo info;

  int ret = read_latest_epoch(dpp, info, y);
  if (ret < 0) {
    return ret;
  }

  latest_epoch = info.epoch;

  return 0;
}

int RGWPeriod::use_latest_epoch(const DoutPrefixProvider *dpp, optional_yield y)
{
  RGWPeriodLatestEpochInfo info;
  int ret = read_latest_epoch(dpp, info, y);
  if (ret < 0) {
    return ret;
  }

  epoch = info.epoch;

  return 0;
}

int RGWPeriod::set_latest_epoch(const DoutPrefixProvider *dpp, 
                                optional_yield y,
				epoch_t epoch, bool exclusive,
                                RGWObjVersionTracker *objv)
{
  string oid = get_period_oid_prefix() + get_latest_epoch_oid();

  rgw_pool pool(get_pool(cct));
  bufferlist bl;

  RGWPeriodLatestEpochInfo info;
  info.epoch = epoch;

  using ceph::encode;
  encode(info, bl);

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));
  return sysobj.wop()
               .set_exclusive(exclusive)
               .write(dpp, bl, y);
}

int RGWPeriod::update_latest_epoch(const DoutPrefixProvider *dpp, epoch_t epoch, optional_yield y)
{
  static constexpr int MAX_RETRIES = 20;

  for (int i = 0; i < MAX_RETRIES; i++) {
    RGWPeriodLatestEpochInfo info;
    RGWObjVersionTracker objv;
    bool exclusive = false;

    // read existing epoch
    int r = read_latest_epoch(dpp, info, y, &objv);
    if (r == -ENOENT) {
      // use an exclusive create to set the epoch atomically
      exclusive = true;
      ldpp_dout(dpp, 20) << "creating initial latest_epoch=" << epoch
          << " for period=" << id << dendl;
    } else if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to read latest_epoch" << dendl;
      return r;
    } else if (epoch <= info.epoch) {
      r = -EEXIST; // fail with EEXIST if epoch is not newer
      ldpp_dout(dpp, 10) << "found existing latest_epoch " << info.epoch
          << " >= given epoch " << epoch << ", returning r=" << r << dendl;
      return r;
    } else {
      ldpp_dout(dpp, 20) << "updating latest_epoch from " << info.epoch
          << " -> " << epoch << " on period=" << id << dendl;
    }

    r = set_latest_epoch(dpp, y, epoch, exclusive, &objv);
    if (r == -EEXIST) {
      continue; // exclusive create raced with another update, retry
    } else if (r == -ECANCELED) {
      continue; // write raced with a conflicting version, retry
    }
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to write latest_epoch" << dendl;
      return r;
    }
    return 0; // return success
  }

  return -ECANCELED; // fail after max retries
}

int RGWPeriod::delete_obj(const DoutPrefixProvider *dpp, optional_yield y)
{
  rgw_pool pool(get_pool(cct));

  // delete the object for each period epoch
  for (epoch_t e = 1; e <= epoch; e++) {
    RGWPeriod p{get_id(), e};
    rgw_raw_obj oid{pool, p.get_period_oid()};
    auto obj_ctx = sysobj_svc->init_obj_ctx();
    auto sysobj = sysobj_svc->get_obj(obj_ctx, oid);
    int ret = sysobj.wop().remove(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "WARNING: failed to delete period object " << oid
          << ": " << cpp_strerror(-ret) << dendl;
    }
  }

  // delete the .latest_epoch object
  rgw_raw_obj oid{pool, get_period_oid_prefix() + get_latest_epoch_oid()};
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, oid);
  int ret = sysobj.wop().remove(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "WARNING: failed to delete period object " << oid
        << ": " << cpp_strerror(-ret) << dendl;
  }
  return ret;
}

int RGWPeriod::read_info(const DoutPrefixProvider *dpp, optional_yield y)
{
  rgw_pool pool(get_pool(cct));

  bufferlist bl;

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, get_period_oid()});
  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failed reading obj info from " << pool << ":" << get_period_oid() << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  try {
    using ceph::decode;
    auto iter = bl.cbegin();
    decode(*this, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode obj from " << pool << ":" << get_period_oid() << dendl;
    return -EIO;
  }

  return 0;
}

int RGWPeriod::create(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive)
{
  int ret;
  
  /* create unique id */
  uuid_d new_uuid;
  char uuid_str[37];
  new_uuid.generate_random();
  new_uuid.print(uuid_str);
  id = uuid_str;

  epoch = FIRST_EPOCH;

  period_map.id = id;

  ret = store_info(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR:  storing info for " << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = set_latest_epoch(dpp, y, epoch);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: setting latest epoch " << id << ": " << cpp_strerror(-ret) << dendl;
  }

  return ret;
}

int RGWPeriod::store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  rgw_pool pool(get_pool(cct));

  string oid = get_period_oid();
  bufferlist bl;
  using ceph::encode;
  encode(*this, bl);

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));
  return sysobj.wop()
               .set_exclusive(exclusive)
               .write(dpp, bl, y);
}

rgw_pool RGWPeriod::get_pool(CephContext *cct) const
{
  if (cct->_conf->rgw_period_root_pool.empty()) {
    return rgw_pool(RGW_DEFAULT_PERIOD_ROOT_POOL);
  }
  return rgw_pool(cct->_conf->rgw_period_root_pool);
}

int RGWPeriod::add_zonegroup(const DoutPrefixProvider *dpp, const RGWZoneGroup& zonegroup, optional_yield y)
{
  if (zonegroup.realm_id != realm_id) {
    return 0;
  }
  int ret = period_map.update(zonegroup, cct);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: updating period map: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return store_info(dpp, false, y);
}

int RGWPeriod::update(const DoutPrefixProvider *dpp, optional_yield y)
{
  auto zone_svc = sysobj_svc->get_zone_svc();
  ldpp_dout(dpp, 20) << __func__ << " realm " << realm_id << " period " << get_id() << dendl;
  list<string> zonegroups;
  int ret = zone_svc->list_zonegroups(dpp, zonegroups);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to list zonegroups: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  // clear zone short ids of removed zones. period_map.update() will add the
  // remaining zones back
  period_map.short_zone_ids.clear();

  for (auto& iter : zonegroups) {
    RGWZoneGroup zg(string(), iter);
    ret = zg.init(dpp, cct, sysobj_svc, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "WARNING: zg.init() failed: " << cpp_strerror(-ret) << dendl;
      continue;
    }

    if (zg.realm_id != realm_id) {
      ldpp_dout(dpp, 20) << "skipping zonegroup " << zg.get_name() << " zone realm id " << zg.realm_id << ", not on our realm " << realm_id << dendl;
      continue;
    }

    if (zg.master_zone.empty()) {
      ldpp_dout(dpp, 0) << "ERROR: zonegroup " << zg.get_name() << " should have a master zone " << dendl;
      return -EINVAL;
    }

    if (zg.zones.find(zg.master_zone) == zg.zones.end()) {
      ldpp_dout(dpp, 0) << "ERROR: zonegroup " << zg.get_name()
                   << " has a non existent master zone "<< dendl;
      return -EINVAL;
    }

    if (zg.is_master_zonegroup()) {
      master_zonegroup = zg.get_id();
      master_zone = zg.master_zone;
    }

    int ret = period_map.update(zg, cct);
    if (ret < 0) {
      return ret;
    }
  }

  ret = period_config.read(dpp, sysobj_svc, realm_id, y);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "ERROR: failed to read period config: "
        << cpp_strerror(ret) << dendl;
    return ret;
  }
  return 0;
}

int RGWPeriod::reflect(const DoutPrefixProvider *dpp, optional_yield y)
{
  for (auto& iter : period_map.zonegroups) {
    RGWZoneGroup& zg = iter.second;
    zg.reinit_instance(cct, sysobj_svc);
    int r = zg.write(dpp, false, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to store zonegroup info for zonegroup=" << iter.first << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    if (zg.is_master_zonegroup()) {
      // set master as default if no default exists
      r = zg.set_as_default(dpp, y, true);
      if (r == 0) {
        ldpp_dout(dpp, 1) << "Set the period's master zonegroup " << zg.get_id()
            << " as the default" << dendl;
      }
    }
  }

  int r = period_config.write(dpp, sysobj_svc, realm_id, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to store period config: "
        << cpp_strerror(-r) << dendl;
    return r;
  }
  return 0;
}

void RGWPeriod::fork()
{
  ldout(cct, 20) << __func__ << " realm " << realm_id << " period " << id << dendl;
  predecessor_uuid = id;
  id = get_staging_id(realm_id);
  period_map.reset();
  realm_epoch++;
}

static int read_sync_status(const DoutPrefixProvider *dpp, rgw::sal::RGWRadosStore *store, rgw_meta_sync_status *sync_status)
{
  // initialize a sync status manager to read the status
  RGWMetaSyncStatusManager mgr(store, store->svc()->rados->get_async_processor());
  int r = mgr.init(dpp);
  if (r < 0) {
    return r;
  }
  r = mgr.read_sync_status(dpp, sync_status);
  mgr.stop();
  return r;
}

int RGWPeriod::update_sync_status(const DoutPrefixProvider *dpp,
                                  rgw::sal::RGWRadosStore *store, /* for now */
				  const RGWPeriod &current_period,
                                  std::ostream& error_stream,
                                  bool force_if_stale)
{
  rgw_meta_sync_status status;
  int r = read_sync_status(dpp, store, &status);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "period failed to read sync status: "
        << cpp_strerror(-r) << dendl;
    return r;
  }

  std::vector<std::string> markers;

  const auto current_epoch = current_period.get_realm_epoch();
  if (current_epoch != status.sync_info.realm_epoch) {
    // no sync status markers for the current period
    ceph_assert(current_epoch > status.sync_info.realm_epoch);
    const int behind = current_epoch - status.sync_info.realm_epoch;
    if (!force_if_stale && current_epoch > 1) {
      error_stream << "ERROR: This zone is " << behind << " period(s) behind "
          "the current master zone in metadata sync. If this zone is promoted "
          "to master, any metadata changes during that time are likely to "
          "be lost.\n"
          "Waiting for this zone to catch up on metadata sync (see "
          "'radosgw-admin sync status') is recommended.\n"
          "To promote this zone to master anyway, add the flag "
          "--yes-i-really-mean-it." << std::endl;
      return -EINVAL;
    }
    // empty sync status markers - other zones will skip this period during
    // incremental metadata sync
    markers.resize(status.sync_info.num_shards);
  } else {
    markers.reserve(status.sync_info.num_shards);
    for (auto& i : status.sync_markers) {
      auto& marker = i.second;
      // filter out markers from other periods
      if (marker.realm_epoch != current_epoch) {
        marker.marker.clear();
      }
      markers.emplace_back(std::move(marker.marker));
    }
  }

  std::swap(sync_status, markers);
  return 0;
}

int RGWPeriod::commit(const DoutPrefixProvider *dpp, 
                      rgw::sal::RGWRadosStore *store,
		      RGWRealm& realm, const RGWPeriod& current_period,
                      std::ostream& error_stream, optional_yield y,
		      bool force_if_stale)
{
  auto zone_svc = sysobj_svc->get_zone_svc();
  ldpp_dout(dpp, 20) << __func__ << " realm " << realm.get_id() << " period " << current_period.get_id() << dendl;
  // gateway must be in the master zone to commit
  if (master_zone != zone_svc->get_zone_params().get_id()) {
    error_stream << "Cannot commit period on zone "
        << zone_svc->get_zone_params().get_id() << ", it must be sent to "
        "the period's master zone " << master_zone << '.' << std::endl;
    return -EINVAL;
  }
  // period predecessor must match current period
  if (predecessor_uuid != current_period.get_id()) {
    error_stream << "Period predecessor " << predecessor_uuid
        << " does not match current period " << current_period.get_id()
        << ". Use 'period pull' to get the latest period from the master, "
        "reapply your changes, and try again." << std::endl;
    return -EINVAL;
  }
  // realm epoch must be 1 greater than current period
  if (realm_epoch != current_period.get_realm_epoch() + 1) {
    error_stream << "Period's realm epoch " << realm_epoch
        << " does not come directly after current realm epoch "
        << current_period.get_realm_epoch() << ". Use 'realm pull' to get the "
        "latest realm and period from the master zone, reapply your changes, "
        "and try again." << std::endl;
    return -EINVAL;
  }
  // did the master zone change?
  if (master_zone != current_period.get_master_zone()) {
    // store the current metadata sync status in the period
    int r = update_sync_status(dpp, store, current_period, error_stream, force_if_stale);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to update metadata sync status: "
          << cpp_strerror(-r) << dendl;
      return r;
    }
    // create an object with a new period id
    r = create(dpp, y, true);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to create new period: " << cpp_strerror(-r) << dendl;
      return r;
    }
    // set as current period
    r = realm.set_current_period(dpp, *this, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to update realm's current period: "
          << cpp_strerror(-r) << dendl;
      return r;
    }
    ldpp_dout(dpp, 4) << "Promoted to master zone and committed new period "
        << id << dendl;
    realm.notify_new_period(dpp, *this, y);
    return 0;
  }
  // period must be based on current epoch
  if (epoch != current_period.get_epoch()) {
    error_stream << "Period epoch " << epoch << " does not match "
        "predecessor epoch " << current_period.get_epoch()
        << ". Use 'period pull' to get the latest epoch from the master zone, "
        "reapply your changes, and try again." << std::endl;
    return -EINVAL;
  }
  // set period as next epoch
  set_id(current_period.get_id());
  set_epoch(current_period.get_epoch() + 1);
  set_predecessor(current_period.get_predecessor());
  realm_epoch = current_period.get_realm_epoch();
  // write the period to rados
  int r = store_info(dpp, false, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to store period: " << cpp_strerror(-r) << dendl;
    return r;
  }
  // set as latest epoch
  r = update_latest_epoch(dpp, epoch, y);
  if (r == -EEXIST) {
    // already have this epoch (or a more recent one)
    return 0;
  }
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to set latest epoch: " << cpp_strerror(-r) << dendl;
    return r;
  }
  r = reflect(dpp, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to update local objects: " << cpp_strerror(-r) << dendl;
    return r;
  }
  ldpp_dout(dpp, 4) << "Committed new epoch " << epoch
      << " for period " << id << dendl;
  realm.notify_new_period(dpp, *this, y);
  return 0;
}

int RGWZoneParams::create_default(const DoutPrefixProvider *dpp, optional_yield y, bool old_format)
{
  name = default_zone_name;

  int r = create(dpp, y);
  if (r < 0) {
    return r;
  }

  if (old_format) {
    name = id;
  }

  return r;
}


namespace {
int get_zones_pool_set(const DoutPrefixProvider *dpp, 
                       CephContext* cct,
                       RGWSI_SysObj* sysobj_svc,
                       const list<string>& zones,
                       const string& my_zone_id,
                       set<rgw_pool>& pool_names,
		       optional_yield y)
{
  for(auto const& iter : zones) {
    RGWZoneParams zone(iter);
    int r = zone.init(dpp, cct, sysobj_svc, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "Error: init zone " << iter << ":" << cpp_strerror(-r) << dendl;
      return r;
    }
    if (zone.get_id() != my_zone_id) {
      pool_names.insert(zone.domain_root);
      pool_names.insert(zone.control_pool);
      pool_names.insert(zone.gc_pool);
      pool_names.insert(zone.log_pool);
      pool_names.insert(zone.intent_log_pool);
      pool_names.insert(zone.usage_log_pool);
      pool_names.insert(zone.user_keys_pool);
      pool_names.insert(zone.user_email_pool);
      pool_names.insert(zone.user_swift_pool);
      pool_names.insert(zone.user_uid_pool);
      pool_names.insert(zone.otp_pool);
      pool_names.insert(zone.roles_pool);
      pool_names.insert(zone.reshard_pool);
      pool_names.insert(zone.notif_pool);
      for(auto& iter : zone.placement_pools) {
	pool_names.insert(iter.second.index_pool);
        for (auto& pi : iter.second.storage_classes.get_all()) {
          if (pi.second.data_pool) {
            pool_names.insert(pi.second.data_pool.get());
          }
        }
	pool_names.insert(iter.second.data_extra_pool);
      }
      pool_names.insert(zone.oidc_pool);
    }
  }
  return 0;
}

rgw_pool fix_zone_pool_dup(set<rgw_pool> pools,
                           const string& default_prefix,
                           const string& default_suffix,
                           const rgw_pool& suggested_pool)
{
  string suggested_name = suggested_pool.to_str();

  string prefix = default_prefix;
  string suffix = default_suffix;

  if (!suggested_pool.empty()) {
    prefix = suggested_name.substr(0, suggested_name.find("."));
    suffix = suggested_name.substr(prefix.length());
  }

  rgw_pool pool(prefix + suffix);
  
  if (pools.find(pool) == pools.end()) {
    return pool;
  } else {
    while(true) {
      pool =  prefix + "_" + std::to_string(std::rand()) + suffix;
      if (pools.find(pool) == pools.end()) {
	return pool;
      }
    }
  }  
}
}

int RGWZoneParams::fix_pool_names(const DoutPrefixProvider *dpp, optional_yield y)
{

  list<string> zones;
  int r = zone_svc->list_zones(dpp, zones);
  if (r < 0) {
    ldpp_dout(dpp, 10) << "WARNING: store->list_zones() returned r=" << r << dendl;
  }

  set<rgw_pool> pools;
  r = get_zones_pool_set(dpp, cct, sysobj_svc, zones, id, pools, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "Error: get_zones_pool_names" << r << dendl;
    return r;
  }

  domain_root = fix_zone_pool_dup(pools, name, ".rgw.meta:root", domain_root);
  control_pool = fix_zone_pool_dup(pools, name, ".rgw.control", control_pool);
  gc_pool = fix_zone_pool_dup(pools, name ,".rgw.log:gc", gc_pool);
  lc_pool = fix_zone_pool_dup(pools, name ,".rgw.log:lc", lc_pool);
  log_pool = fix_zone_pool_dup(pools, name, ".rgw.log", log_pool);
  intent_log_pool = fix_zone_pool_dup(pools, name, ".rgw.log:intent", intent_log_pool);
  usage_log_pool = fix_zone_pool_dup(pools, name, ".rgw.log:usage", usage_log_pool);
  user_keys_pool = fix_zone_pool_dup(pools, name, ".rgw.meta:users.keys", user_keys_pool);
  user_email_pool = fix_zone_pool_dup(pools, name, ".rgw.meta:users.email", user_email_pool);
  user_swift_pool = fix_zone_pool_dup(pools, name, ".rgw.meta:users.swift", user_swift_pool);
  user_uid_pool = fix_zone_pool_dup(pools, name, ".rgw.meta:users.uid", user_uid_pool);
  roles_pool = fix_zone_pool_dup(pools, name, ".rgw.meta:roles", roles_pool);
  reshard_pool = fix_zone_pool_dup(pools, name, ".rgw.log:reshard", reshard_pool);
  otp_pool = fix_zone_pool_dup(pools, name, ".rgw.otp", otp_pool);
  oidc_pool = fix_zone_pool_dup(pools, name, ".rgw.meta:oidc", oidc_pool);
  notif_pool = fix_zone_pool_dup(pools, name ,".rgw.log:notif", notif_pool);

  for(auto& iter : placement_pools) {
    iter.second.index_pool = fix_zone_pool_dup(pools, name, "." + default_bucket_index_pool_suffix,
                                               iter.second.index_pool);
    for (auto& pi : iter.second.storage_classes.get_all()) {
      if (pi.second.data_pool) {
        rgw_pool& pool = pi.second.data_pool.get();
        pool = fix_zone_pool_dup(pools, name, "." + default_storage_pool_suffix,
                                 pool);
      }
    }
    iter.second.data_extra_pool= fix_zone_pool_dup(pools, name, "." + default_storage_extra_pool_suffix,
                                                   iter.second.data_extra_pool);
  }

  return 0;
}

int RGWZoneParams::create(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive)
{
  /* check for old pools config */
  rgw_raw_obj obj(domain_root, avail_pools);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, obj);
  int r = sysobj.rop().stat(y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 10) << "couldn't find old data placement pools config, setting up new ones for the zone" << dendl;
    /* a new system, let's set new placement info */
    RGWZonePlacementInfo default_placement;
    default_placement.index_pool = name + "." + default_bucket_index_pool_suffix;
    rgw_pool pool = name + "." + default_storage_pool_suffix;
    default_placement.storage_classes.set_storage_class(RGW_STORAGE_CLASS_STANDARD, &pool, nullptr);
    default_placement.data_extra_pool = name + "." + default_storage_extra_pool_suffix;
    placement_pools["default-placement"] = default_placement;
  }

  r = fix_pool_names(dpp, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: fix_pool_names returned r=" << r << dendl;
    return r;
  }

  r = RGWSystemMetaObj::create(dpp, y, exclusive);
  if (r < 0) {
    return r;
  }

  // try to set as default. may race with another create, so pass exclusive=true
  // so we don't override an existing default
  r = set_as_default(dpp, y, true);
  if (r < 0 && r != -EEXIST) {
    ldpp_dout(dpp, 10) << "WARNING: failed to set zone as default, r=" << r << dendl;
  }

  return 0;
}

rgw_pool RGWZoneParams::get_pool(CephContext *cct) const
{
  if (cct->_conf->rgw_zone_root_pool.empty()) {
    return rgw_pool(RGW_DEFAULT_ZONE_ROOT_POOL);
  }

  return rgw_pool(cct->_conf->rgw_zone_root_pool);
}

const string RGWZoneParams::get_default_oid(bool old_format) const
{
  if (old_format) {
    return cct->_conf->rgw_default_zone_info_oid;
  }

  return cct->_conf->rgw_default_zone_info_oid + "." + realm_id;
}

const string& RGWZoneParams::get_names_oid_prefix() const
{
  return zone_names_oid_prefix;
}

const string& RGWZoneParams::get_info_oid_prefix(bool old_format) const
{
  return zone_info_oid_prefix;
}

const string& RGWZoneParams::get_predefined_name(CephContext *cct) const {
  return cct->_conf->rgw_zone;
}

int RGWZoneParams::init(const DoutPrefixProvider *dpp, 
                        CephContext *cct, RGWSI_SysObj *sysobj_svc,
			optional_yield y, bool setup_obj, bool old_format)
{
  if (name.empty()) {
    name = cct->_conf->rgw_zone;
  }

  return RGWSystemMetaObj::init(dpp, cct, sysobj_svc, y, setup_obj, old_format);
}

int RGWZoneParams::read_default_id(const DoutPrefixProvider *dpp, string& default_id, optional_yield y,
				   bool old_format)
{
  if (realm_id.empty()) {
    /* try using default realm */
    RGWRealm realm;
    int ret = realm.init(dpp, cct, sysobj_svc, y);
    //no default realm exist
    if (ret < 0) {
      return read_id(dpp, default_zone_name, default_id, y);
    }
    realm_id = realm.get_id();
  }

  return RGWSystemMetaObj::read_default_id(dpp, default_id, y, old_format);
}


int RGWZoneParams::set_as_default(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive)
{
  if (realm_id.empty()) {
    /* try using default realm */
    RGWRealm realm;
    int ret = realm.init(dpp, cct, sysobj_svc, y);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "could not read realm id: " << cpp_strerror(-ret) << dendl;
      return -EINVAL;
    }
    realm_id = realm.get_id();
  }

  return RGWSystemMetaObj::set_as_default(dpp, y, exclusive);
}

const string& RGWZoneParams::get_compression_type(const rgw_placement_rule& placement_rule) const
{
  static const std::string NONE{"none"};
  auto p = placement_pools.find(placement_rule.name);
  if (p == placement_pools.end()) {
    return NONE;
  }
  const auto& type = p->second.get_compression_type(placement_rule.get_storage_class());
  return !type.empty() ? type : NONE;
}

void RGWPeriodMap::encode(bufferlist& bl) const {
  ENCODE_START(2, 1, bl);
  encode(id, bl);
  encode(zonegroups, bl);
  encode(master_zonegroup, bl);
  encode(short_zone_ids, bl);
  ENCODE_FINISH(bl);
}

void RGWPeriodMap::decode(bufferlist::const_iterator& bl) {
  DECODE_START(2, bl);
  decode(id, bl);
  decode(zonegroups, bl);
  decode(master_zonegroup, bl);
  if (struct_v >= 2) {
    decode(short_zone_ids, bl);
  }
  DECODE_FINISH(bl);

  zonegroups_by_api.clear();
  for (map<string, RGWZoneGroup>::iterator iter = zonegroups.begin();
       iter != zonegroups.end(); ++iter) {
    RGWZoneGroup& zonegroup = iter->second;
    zonegroups_by_api[zonegroup.api_name] = zonegroup;
    if (zonegroup.is_master_zonegroup()) {
      master_zonegroup = zonegroup.get_id();
    }
  }
}

// run an MD5 hash on the zone_id and return the first 32 bits
static uint32_t gen_short_zone_id(const std::string zone_id)
{
  unsigned char md5[CEPH_CRYPTO_MD5_DIGESTSIZE];
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  hash.Update((const unsigned char *)zone_id.c_str(), zone_id.size());
  hash.Final(md5);

  uint32_t short_id;
  memcpy((char *)&short_id, md5, sizeof(short_id));
  return std::max(short_id, 1u);
}

int RGWPeriodMap::update(const RGWZoneGroup& zonegroup, CephContext *cct)
{
  if (zonegroup.is_master_zonegroup() && (!master_zonegroup.empty() && zonegroup.get_id() != master_zonegroup)) {
    ldout(cct,0) << "Error updating periodmap, multiple master zonegroups configured "<< dendl;
    ldout(cct,0) << "master zonegroup: " << master_zonegroup << " and  " << zonegroup.get_id() <<dendl;
    return -EINVAL;
  }
  map<string, RGWZoneGroup>::iterator iter = zonegroups.find(zonegroup.get_id());
  if (iter != zonegroups.end()) {
    RGWZoneGroup& old_zonegroup = iter->second;
    if (!old_zonegroup.api_name.empty()) {
      zonegroups_by_api.erase(old_zonegroup.api_name);
    }
  }
  zonegroups[zonegroup.get_id()] = zonegroup;

  if (!zonegroup.api_name.empty()) {
    zonegroups_by_api[zonegroup.api_name] = zonegroup;
  }

  if (zonegroup.is_master_zonegroup()) {
    master_zonegroup = zonegroup.get_id();
  } else if (master_zonegroup == zonegroup.get_id()) {
    master_zonegroup = "";
  }

  for (auto& i : zonegroup.zones) {
    auto& zone = i.second;
    if (short_zone_ids.find(zone.id) != short_zone_ids.end()) {
      continue;
    }
    // calculate the zone's short id
    uint32_t short_id = gen_short_zone_id(zone.id);

    // search for an existing zone with the same short id
    for (auto& s : short_zone_ids) {
      if (s.second == short_id) {
        ldout(cct, 0) << "New zone '" << zone.name << "' (" << zone.id
            << ") generates the same short_zone_id " << short_id
            << " as existing zone id " << s.first << dendl;
        return -EEXIST;
      }
    }

    short_zone_ids[zone.id] = short_id;
  }

  return 0;
}

uint32_t RGWPeriodMap::get_zone_short_id(const string& zone_id) const
{
  auto i = short_zone_ids.find(zone_id);
  if (i == short_zone_ids.end()) {
    return 0;
  }
  return i->second;
}

int RGWZoneGroupMap::read(const DoutPrefixProvider *dpp, CephContext *cct, RGWSI_SysObj *sysobj_svc, optional_yield y)
{

  RGWPeriod period;
  int ret = period.init(dpp, cct, sysobj_svc, y);
  if (ret < 0) {
    cerr << "failed to read current period info: " << cpp_strerror(ret);
    return ret;
  }

  bucket_quota = period.get_config().bucket_quota;
  user_quota = period.get_config().user_quota;
  zonegroups = period.get_map().zonegroups;
  zonegroups_by_api = period.get_map().zonegroups_by_api;
  master_zonegroup = period.get_map().master_zonegroup;

  return 0;
}

void RGWRegionMap::encode(bufferlist& bl) const {
  ENCODE_START( 3, 1, bl);
  encode(regions, bl);
  encode(master_region, bl);
  encode(bucket_quota, bl);
  encode(user_quota, bl);
  ENCODE_FINISH(bl);
}

void RGWRegionMap::decode(bufferlist::const_iterator& bl) {
  DECODE_START(3, bl);
  decode(regions, bl);
  decode(master_region, bl);
  if (struct_v >= 2)
    decode(bucket_quota, bl);
  if (struct_v >= 3)
    decode(user_quota, bl);
  DECODE_FINISH(bl);
}

void RGWZoneGroupMap::encode(bufferlist& bl) const {
  ENCODE_START( 3, 1, bl);
  encode(zonegroups, bl);
  encode(master_zonegroup, bl);
  encode(bucket_quota, bl);
  encode(user_quota, bl);
  ENCODE_FINISH(bl);
}

void RGWZoneGroupMap::decode(bufferlist::const_iterator& bl) {
  DECODE_START(3, bl);
  decode(zonegroups, bl);
  decode(master_zonegroup, bl);
  if (struct_v >= 2)
    decode(bucket_quota, bl);
  if (struct_v >= 3)
    decode(user_quota, bl);
  DECODE_FINISH(bl);

  zonegroups_by_api.clear();
  for (map<string, RGWZoneGroup>::iterator iter = zonegroups.begin();
       iter != zonegroups.end(); ++iter) {
    RGWZoneGroup& zonegroup = iter->second;
    zonegroups_by_api[zonegroup.api_name] = zonegroup;
    if (zonegroup.is_master_zonegroup()) {
      master_zonegroup = zonegroup.get_name();
    }
  }
}


