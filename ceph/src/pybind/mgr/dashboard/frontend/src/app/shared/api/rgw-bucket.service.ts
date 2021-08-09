import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { of as observableOf } from 'rxjs';
import { catchError, mapTo } from 'rxjs/operators';

import { cdEncode } from '../decorators/cd-encode';
import { ApiModule } from './api.module';

@cdEncode
@Injectable({
  providedIn: ApiModule
})
export class RgwBucketService {
  private url = 'api/rgw/bucket';

  constructor(private http: HttpClient) {}

  /**
   * Get the list of buckets.
   * @return {Observable<Object[]>}
   */
  list() {
    let params = new HttpParams();
    params = params.append('stats', 'true');
    return this.http.get(this.url, { params: params });
  }

  get(bucket: string) {
    return this.http.get(`${this.url}/${bucket}`);
  }

  create(
    bucket: string,
    uid: string,
    zonegroup: string,
    placementTarget: string,
    lockEnabled: boolean,
    lock_mode: 'GOVERNANCE' | 'COMPLIANCE',
    lock_retention_period_days: string,
    lock_retention_period_years: string
  ) {
    return this.http.post(this.url, null, {
      params: new HttpParams({
        fromObject: {
          bucket,
          uid,
          zonegroup,
          placement_target: placementTarget,
          lock_enabled: String(lockEnabled),
          lock_mode,
          lock_retention_period_days,
          lock_retention_period_years
        }
      })
    });
  }

  update(
    bucket: string,
    bucketId: string,
    uid: string,
    versioningState: string,
    mfaDelete: string,
    mfaTokenSerial: string,
    mfaTokenPin: string,
    lockMode: 'GOVERNANCE' | 'COMPLIANCE',
    lockRetentionPeriodDays: string,
    lockRetentionPeriodYears: string
  ) {
    let params = new HttpParams();
    params = params.append('bucket_id', bucketId);
    params = params.append('uid', uid);
    params = params.append('versioning_state', versioningState);
    params = params.append('mfa_delete', mfaDelete);
    params = params.append('mfa_token_serial', mfaTokenSerial);
    params = params.append('mfa_token_pin', mfaTokenPin);
    params = params.append('lock_mode', lockMode);
    params = params.append('lock_retention_period_days', lockRetentionPeriodDays);
    params = params.append('lock_retention_period_years', lockRetentionPeriodYears);
    return this.http.put(`${this.url}/${bucket}`, null, { params: params });
  }

  delete(bucket: string, purgeObjects = true) {
    let params = new HttpParams();
    params = params.append('purge_objects', purgeObjects ? 'true' : 'false');
    return this.http.delete(`${this.url}/${bucket}`, { params: params });
  }

  /**
   * Check if the specified bucket exists.
   * @param {string} bucket The bucket name to check.
   * @return {Observable<boolean>}
   */
  exists(bucket: string) {
    return this.get(bucket).pipe(
      mapTo(true),
      catchError((error: Event) => {
        if (_.isFunction(error.preventDefault)) {
          error.preventDefault();
        }
        return observableOf(false);
      })
    );
  }
}
