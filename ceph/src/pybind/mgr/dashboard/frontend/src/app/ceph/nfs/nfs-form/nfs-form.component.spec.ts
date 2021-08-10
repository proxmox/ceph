import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { ActivatedRoute } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { TypeaheadModule } from 'ngx-bootstrap/typeahead';
import { ToastrModule } from 'ngx-toastr';

import { ActivatedRouteStub } from '../../../../testing/activated-route-stub';
import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { NFSClusterType } from '../nfs-cluster-type.enum';
import { NfsFormClientComponent } from '../nfs-form-client/nfs-form-client.component';
import { NfsFormComponent } from './nfs-form.component';

describe('NfsFormComponent', () => {
  let component: NfsFormComponent;
  let fixture: ComponentFixture<NfsFormComponent>;
  let httpTesting: HttpTestingController;
  let activatedRoute: ActivatedRouteStub;

  configureTestBed({
    declarations: [NfsFormComponent, NfsFormClientComponent],
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot(),
      TypeaheadModule.forRoot()
    ],
    providers: [
      {
        provide: ActivatedRoute,
        useValue: new ActivatedRouteStub({ cluster_id: undefined, export_id: undefined })
      },
      i18nProviders
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NfsFormComponent);
    component = fixture.componentInstance;
    httpTesting = TestBed.get(HttpTestingController);
    activatedRoute = TestBed.get(ActivatedRoute);
    fixture.detectChanges();

    httpTesting.expectOne('api/nfs-ganesha/daemon').flush([
      { daemon_id: 'node1', cluster_id: 'cluster1', cluster_type: NFSClusterType.user },
      { daemon_id: 'node2', cluster_id: 'cluster1', cluster_type: NFSClusterType.user },
      { daemon_id: 'node5', cluster_id: 'cluster2', cluster_type: NFSClusterType.orchestrator }
    ]);
    httpTesting.expectOne('ui-api/nfs-ganesha/fsals').flush(['CEPH', 'RGW']);
    httpTesting.expectOne('ui-api/nfs-ganesha/cephx/clients').flush(['admin', 'fs', 'rgw']);
    httpTesting.expectOne('ui-api/nfs-ganesha/cephfs/filesystems').flush([{ id: 1, name: 'a' }]);
    httpTesting.expectOne('api/rgw/user').flush(['test', 'dev']);
    const user_dev = {
      suspended: 0,
      user_id: 'dev',
      keys: ['a']
    };
    httpTesting.expectOne('api/rgw/user/dev').flush(user_dev);
    const user_test = {
      suspended: 1,
      user_id: 'test',
      keys: ['a']
    };
    httpTesting.expectOne('api/rgw/user/test').flush(user_test);
    httpTesting.verify();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should process all data', () => {
    expect(component.allDaemons).toEqual({ cluster1: ['node1', 'node2'], cluster2: ['node5'] });
    expect(component.isDefaultCluster).toEqual(false);
    expect(component.allFsals).toEqual([
      { descr: 'CephFS', value: 'CEPH' },
      { descr: 'Object Gateway', value: 'RGW' }
    ]);
    expect(component.allCephxClients).toEqual(['admin', 'fs', 'rgw']);
    expect(component.allFsNames).toEqual([{ id: 1, name: 'a' }]);
    expect(component.allRgwUsers).toEqual(['dev']);
  });

  it('should create the form', () => {
    expect(component.nfsForm.value).toEqual({
      access_type: 'RW',
      clients: [],
      cluster_id: '',
      daemons: [],
      fsal: { fs_name: 'a', name: '', rgw_user_id: '', user_id: '' },
      path: '',
      protocolNfsv3: false,
      protocolNfsv4: true,
      pseudo: '',
      sec_label_xattr: 'security.selinux',
      security_label: false,
      squash: '',
      tag: '',
      transportTCP: true,
      transportUDP: true
    });
    expect(component.nfsForm.get('cluster_id').disabled).toBeFalsy();
  });

  it('should prepare data when selecting an cluster', () => {
    expect(component.allDaemons).toEqual({ cluster1: ['node1', 'node2'], cluster2: ['node5'] });
    expect(component.daemonsSelections).toEqual([]);
    expect(component.clusterType).toBeNull();

    component.nfsForm.patchValue({ cluster_id: 'cluster1' });
    component.onClusterChange();

    expect(component.daemonsSelections).toEqual([
      { description: '', name: 'node1', selected: false, enabled: true },
      { description: '', name: 'node2', selected: false, enabled: true }
    ]);
    expect(component.clusterType).toBe(NFSClusterType.user);

    component.nfsForm.patchValue({ cluster_id: 'cluster2' });
    component.onClusterChange();
    expect(component.clusterType).toBe(NFSClusterType.orchestrator);
    expect(component.daemonsSelections).toEqual([]);
  });

  it('should clean data when changing cluster', () => {
    component.nfsForm.patchValue({ cluster_id: 'cluster1', daemons: ['node1'] });
    component.nfsForm.patchValue({ cluster_id: 'node2' });
    component.onClusterChange();

    expect(component.nfsForm.getValue('daemons')).toEqual([]);
  });

  it('should not allow changing cluster in edit mode', () => {
    component.isEdit = true;
    component.ngOnInit();
    expect(component.nfsForm.get('cluster_id').disabled).toBeTruthy();
  });

  it('should mark NFSv4 protocol as required', () => {
    component.nfsForm.patchValue({
      protocolNfsv4: false
    });
    component.nfsForm.updateValueAndValidity({ emitEvent: false });
    expect(component.nfsForm.valid).toBeFalsy();
    expect(component.nfsForm.get('protocolNfsv4').hasError('required')).toBeTruthy();
  });

  describe('should submit request', () => {
    beforeEach(() => {
      component.nfsForm.patchValue({
        access_type: 'RW',
        clients: [],
        cluster_id: 'cluster1',
        daemons: ['node2'],
        fsal: { name: 'CEPH', user_id: 'fs', fs_name: 1, rgw_user_id: '' },
        path: '/foo',
        protocolNfsv3: true,
        protocolNfsv4: true,
        pseudo: '/baz',
        squash: 'no_root_squash',
        tag: 'bar',
        transportTCP: true,
        transportUDP: true
      });
    });

    it('should remove "pseudo" requirement when NFS v4 disabled', () => {
      component.nfsForm.patchValue({
        protocolNfsv4: false,
        pseudo: ''
      });

      component.nfsForm.updateValueAndValidity({ emitEvent: false });
      expect(component.nfsForm.valid).toBeTruthy();
    });

    it('should call update', () => {
      activatedRoute.setParams({ cluster_id: 'cluster1', export_id: '1' });
      component.isEdit = true;
      component.cluster_id = 'cluster1';
      component.export_id = '1';
      component.nfsForm.patchValue({ export_id: 1 });
      component.submitAction();

      const req = httpTesting.expectOne('api/nfs-ganesha/export/cluster1/1');
      expect(req.request.method).toBe('PUT');
      expect(req.request.body).toEqual({
        access_type: 'RW',
        clients: [],
        cluster_id: 'cluster1',
        daemons: ['node2'],
        export_id: '1',
        fsal: { fs_name: 1, name: 'CEPH', sec_label_xattr: null, user_id: 'fs' },
        path: '/foo',
        protocols: [3, 4],
        pseudo: '/baz',
        security_label: false,
        squash: 'no_root_squash',
        tag: 'bar',
        transports: ['TCP', 'UDP']
      });
    });

    it('should call create', () => {
      activatedRoute.setParams({ cluster_id: undefined, export_id: undefined });
      component.submitAction();

      const req = httpTesting.expectOne('api/nfs-ganesha/export');
      expect(req.request.method).toBe('POST');
      expect(req.request.body).toEqual({
        access_type: 'RW',
        clients: [],
        cluster_id: 'cluster1',
        daemons: ['node2'],
        fsal: {
          fs_name: 1,
          name: 'CEPH',
          sec_label_xattr: null,
          user_id: 'fs'
        },
        path: '/foo',
        protocols: [3, 4],
        pseudo: '/baz',
        security_label: false,
        squash: 'no_root_squash',
        tag: 'bar',
        transports: ['TCP', 'UDP']
      });
    });
  });
});
