// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_ACL_H
#define CEPH_RGW_ACL_H

#include <map>
#include <string>
#include <string_view>
#include <include/types.h>

#include <boost/optional.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "common/debug.h"

#include "rgw_basic_types.h" //includes rgw_acl_types.h

class ACLGrant
{
protected:
  ACLGranteeType type;
  rgw_user id;
  std::string email;
  mutable rgw_user email_id;
  ACLPermission permission;
  std::string name;
  ACLGroupTypeEnum group;
  std::string url_spec;

public:
  ACLGrant() : group(ACL_GROUP_NONE) {}
  virtual ~ACLGrant() {}

  /* there's an assumption here that email/uri/id encodings are
     different and there can't be any overlap */
  bool get_id(rgw_user& _id) const {
    switch(type.get_type()) {
    case ACL_TYPE_EMAIL_USER:
      _id = email; // implies from_str() that parses the 't:u' syntax
      return true;
    case ACL_TYPE_GROUP:
    case ACL_TYPE_REFERER:
      return false;
    default:
      _id = id;
      return true;
    }
  }

  const rgw_user* get_id() const {
    switch(type.get_type()) {
    case ACL_TYPE_EMAIL_USER:
      email_id.from_str(email);
      return &email_id;
    case ACL_TYPE_GROUP:
    case ACL_TYPE_REFERER:
      return nullptr;
    default:
      return &id;
    }
  }

  ACLGranteeType& get_type() { return type; }
  const ACLGranteeType& get_type() const { return type; }
  ACLPermission& get_permission() { return permission; }
  const ACLPermission& get_permission() const { return permission; }
  ACLGroupTypeEnum get_group() const { return group; }
  const std::string& get_referer() const { return url_spec; }

  void encode(bufferlist& bl) const {
    ENCODE_START(5, 3, bl);
    encode(type, bl);
    std::string s;
    id.to_str(s);
    encode(s, bl);
    std::string uri;
    encode(uri, bl);
    encode(email, bl);
    encode(permission, bl);
    encode(name, bl);
    __u32 g = (__u32)group;
    encode(g, bl);
    encode(url_spec, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(5, 3, 3, bl);
    decode(type, bl);
    std::string s;
    decode(s, bl);
    id.from_str(s);
    std::string uri;
    decode(uri, bl);
    decode(email, bl);
    decode(permission, bl);
    decode(name, bl);
    if (struct_v > 1) {
      __u32 g;
      decode(g, bl);
      group = (ACLGroupTypeEnum)g;
    } else {
      group = uri_to_group(uri);
    }
    if (struct_v >= 5) {
      decode(url_spec, bl);
    } else {
      url_spec.clear();
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<ACLGrant*>& o);

  ACLGroupTypeEnum uri_to_group(std::string& uri);

  void set_canon(const rgw_user& _id, const std::string& _name, const uint32_t perm) {
    type.set(ACL_TYPE_CANON_USER);
    id = _id;
    name = _name;
    permission.set_permissions(perm);
  }
  void set_group(ACLGroupTypeEnum _group, const uint32_t perm) {
    type.set(ACL_TYPE_GROUP);
    group = _group;
    permission.set_permissions(perm);
  }
  void set_referer(const std::string& _url_spec, const uint32_t perm) {
    type.set(ACL_TYPE_REFERER);
    url_spec = _url_spec;
    permission.set_permissions(perm);
  }

  friend bool operator==(const ACLGrant& lhs, const ACLGrant& rhs);
  friend bool operator!=(const ACLGrant& lhs, const ACLGrant& rhs);
};
WRITE_CLASS_ENCODER(ACLGrant)

struct ACLReferer {
  std::string url_spec;
  uint32_t perm;

  ACLReferer() : perm(0) {}
  ACLReferer(const std::string& url_spec,
             const uint32_t perm)
    : url_spec(url_spec),
      perm(perm) {
  }

  bool is_match(std::string_view http_referer) const {
    const auto http_host = get_http_host(http_referer);
    if (!http_host || http_host->length() < url_spec.length()) {
      return false;
    }

    if ("*" == url_spec) {
      return true;
    }

    if (http_host->compare(url_spec) == 0) {
      return true;
    }

    if ('.' == url_spec[0]) {
      /* Wildcard support: a referer matches the spec when its last char are
       * perfectly equal to spec. */
      return boost::algorithm::ends_with(http_host.value(), url_spec);
    }

    return false;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(url_spec, bl);
    encode(perm, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    decode(url_spec, bl);
    decode(perm, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;

  friend bool operator==(const ACLReferer& lhs, const ACLReferer& rhs);
  friend bool operator!=(const ACLReferer& lhs, const ACLReferer& rhs);

private:
  boost::optional<std::string_view> get_http_host(const std::string_view url) const {
    size_t pos = url.find("://");
    if (pos == std::string_view::npos || boost::algorithm::starts_with(url, "://") ||
        boost::algorithm::ends_with(url, "://") || boost::algorithm::ends_with(url, "@")) {
      return boost::none;
    }
    std::string_view url_sub = url.substr(pos + strlen("://"));
    pos = url_sub.find('@');
    if (pos != std::string_view::npos) {
      url_sub = url_sub.substr(pos + 1);
    }
    pos = url_sub.find_first_of("/:");
    if (pos == std::string_view::npos) {
      /* no port or path exists */
      return url_sub;
    }
    return url_sub.substr(0, pos);
  }
};
WRITE_CLASS_ENCODER(ACLReferer)

namespace rgw {
namespace auth {
  class Identity;
}
}

using ACLGrantMap = std::multimap<std::string, ACLGrant>;

class RGWAccessControlList
{
protected:
  CephContext *cct;
  /* FIXME: in the feature we should consider switching to uint32_t also
   * in data structures. */
  std::map<std::string, int> acl_user_map;
  std::map<uint32_t, int> acl_group_map;
  std::list<ACLReferer> referer_list;
  ACLGrantMap grant_map;
  void _add_grant(ACLGrant *grant);
public:
  explicit RGWAccessControlList(CephContext *_cct) : cct(_cct) {}
  RGWAccessControlList() : cct(NULL) {}

  void set_ctx(CephContext *ctx) {
    cct = ctx;
  }

  virtual ~RGWAccessControlList() {}

  uint32_t get_perm(const DoutPrefixProvider* dpp,
                    const rgw::auth::Identity& auth_identity,
                    uint32_t perm_mask);
  uint32_t get_group_perm(const DoutPrefixProvider *dpp, ACLGroupTypeEnum group, uint32_t perm_mask) const;
  uint32_t get_referer_perm(const DoutPrefixProvider *dpp, uint32_t current_perm,
                            std::string http_referer,
                            uint32_t perm_mask);
  void encode(bufferlist& bl) const {
    ENCODE_START(4, 3, bl);
    bool maps_initialized = true;
    encode(maps_initialized, bl);
    encode(acl_user_map, bl);
    encode(grant_map, bl);
    encode(acl_group_map, bl);
    encode(referer_list, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, bl);
    bool maps_initialized;
    decode(maps_initialized, bl);
    decode(acl_user_map, bl);
    decode(grant_map, bl);
    if (struct_v >= 2) {
      decode(acl_group_map, bl);
    } else if (!maps_initialized) {
      ACLGrantMap::iterator iter;
      for (iter = grant_map.begin(); iter != grant_map.end(); ++iter) {
        ACLGrant& grant = iter->second;
        _add_grant(&grant);
      }
    }
    if (struct_v >= 4) {
      decode(referer_list, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWAccessControlList*>& o);

  void add_grant(ACLGrant *grant);
  void remove_canon_user_grant(rgw_user& user_id);

  ACLGrantMap& get_grant_map() { return grant_map; }
  const ACLGrantMap& get_grant_map() const { return grant_map; }

  void create_default(const rgw_user& id, std::string name) {
    acl_user_map.clear();
    acl_group_map.clear();
    referer_list.clear();

    ACLGrant grant;
    grant.set_canon(id, name, RGW_PERM_FULL_CONTROL);
    add_grant(&grant);
  }

  friend bool operator==(const RGWAccessControlList& lhs, const RGWAccessControlList& rhs);
  friend bool operator!=(const RGWAccessControlList& lhs, const RGWAccessControlList& rhs);
};
WRITE_CLASS_ENCODER(RGWAccessControlList)

class ACLOwner
{
protected:
  rgw_user id;
  std::string display_name;
public:
  ACLOwner() {}
  ACLOwner(const rgw_user& _id) : id(_id) {}
  ~ACLOwner() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 2, bl);
    std::string s;
    id.to_str(s);
    encode(s, bl);
    encode(display_name, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
    std::string s;
    decode(s, bl);
    id.from_str(s);
    decode(display_name, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<ACLOwner*>& o);
  void set_id(const rgw_user& _id) { id = _id; }
  void set_name(const std::string& name) { display_name = name; }

  rgw_user& get_id() { return id; }
  const rgw_user& get_id() const { return id; }
  std::string& get_display_name() { return display_name; }
  const std::string& get_display_name() const { return display_name; }
  friend bool operator==(const ACLOwner& lhs, const ACLOwner& rhs);
  friend bool operator!=(const ACLOwner& lhs, const ACLOwner& rhs);
};
WRITE_CLASS_ENCODER(ACLOwner)

class RGWAccessControlPolicy
{
protected:
  CephContext *cct;
  RGWAccessControlList acl;
  ACLOwner owner;

public:
  explicit RGWAccessControlPolicy(CephContext *_cct) : cct(_cct), acl(_cct) {}
  RGWAccessControlPolicy() : cct(NULL), acl(NULL) {}
  virtual ~RGWAccessControlPolicy() {}

  void set_ctx(CephContext *ctx) {
    cct = ctx;
    acl.set_ctx(ctx);
  }

  uint32_t get_perm(const DoutPrefixProvider* dpp,
                    const rgw::auth::Identity& auth_identity,
                    uint32_t perm_mask,
                    const char * http_referer,
                    bool ignore_public_acls=false);
  bool verify_permission(const DoutPrefixProvider* dpp,
                         const rgw::auth::Identity& auth_identity,
                         uint32_t user_perm_mask,
                         uint32_t perm,
                         const char * http_referer = nullptr,
                         bool ignore_public_acls=false);

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    encode(owner, bl);
    encode(acl, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    decode(owner, bl);
    decode(acl, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWAccessControlPolicy*>& o);
  void decode_owner(bufferlist::const_iterator& bl) { // sometimes we only need that, should be faster
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    decode(owner, bl);
    DECODE_FINISH(bl);
  }

  void set_owner(ACLOwner& o) { owner = o; }
  ACLOwner& get_owner() {
    return owner;
  }

  void create_default(const rgw_user& id, std::string& name) {
    acl.create_default(id, name);
    owner.set_id(id);
    owner.set_name(name);
  }
  RGWAccessControlList& get_acl() {
    return acl;
  }
  const RGWAccessControlList& get_acl() const {
    return acl;
  }

  virtual bool compare_group_name(std::string& id, ACLGroupTypeEnum group) { return false; }
  bool is_public(const DoutPrefixProvider *dpp) const;

  friend bool operator==(const RGWAccessControlPolicy& lhs, const RGWAccessControlPolicy& rhs);
  friend bool operator!=(const RGWAccessControlPolicy& lhs, const RGWAccessControlPolicy& rhs);
};
WRITE_CLASS_ENCODER(RGWAccessControlPolicy)

#endif
