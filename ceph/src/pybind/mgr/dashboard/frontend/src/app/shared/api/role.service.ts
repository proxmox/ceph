import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable, of as observableOf } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { RoleFormModel } from '../../core/auth/role-form/role-form.model';
import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class RoleService {
  constructor(private http: HttpClient) {}

  list() {
    return this.http.get('api/role');
  }

  delete(name: string) {
    return this.http.delete(`api/role/${name}`);
  }

  get(name: string) {
    return this.http.get(`api/role/${name}`);
  }

  create(role: RoleFormModel) {
    return this.http.post(`api/role`, role);
  }

  clone(name: string, newName: string) {
    return this.http.post(`api/role/${name}/clone`, { new_name: newName });
  }

  update(role: RoleFormModel) {
    return this.http.put(`api/role/${role.name}`, role);
  }

  exists(name: string): Observable<boolean> {
    return this.list().pipe(
      mergeMap((roles: Array<RoleFormModel>) => {
        const exists = roles.some((currentRole: RoleFormModel) => {
          return currentRole.name === name;
        });
        return observableOf(exists);
      })
    );
  }
}
