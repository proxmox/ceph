import { Component, OnInit, ViewChild } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { Router } from '@angular/router';
import _ from 'lodash';

import { NgbNav, NgbTooltip, NgbTypeahead } from '@ng-bootstrap/ng-bootstrap';
import { merge, Observable, Subject } from 'rxjs';
import { debounceTime, distinctUntilChanged, filter, map } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { SelectMessages } from '~/app/shared/components/select/select-messages.model';
import { SelectOption } from '~/app/shared/components/select/select-option.model';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Permission } from '~/app/shared/models/permissions';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';

@Component({
  selector: 'cd-cephfs-form',
  templateUrl: './cephfs-form.component.html',
  styleUrls: ['./cephfs-form.component.scss']
})
export class CephfsVolumeFormComponent extends CdForm implements OnInit {
  @ViewChild('crushInfoTabs') crushInfoTabs: NgbNav;
  @ViewChild('crushDeletionBtn') crushDeletionBtn: NgbTooltip;
  @ViewChild('ecpInfoTabs') ecpInfoTabs: NgbNav;
  @ViewChild('ecpDeletionBtn') ecpDeletionBtn: NgbTooltip;
  @ViewChild(NgbTypeahead, { static: false })
  typeahead: NgbTypeahead;

  labelFocus = new Subject<string>();
  labelClick = new Subject<string>();

  orchStatus$: Observable<any>;

  permission: Permission;
  form: CdFormGroup;
  action: string;
  resource: string;
  editing: boolean;
  icons = Icons;
  hosts: any;
  labels: string[];
  hasOrchestrator: boolean;

  constructor(
    private router: Router,
    private taskWrapperService: TaskWrapperService,
    private orchService: OrchestratorService,
    private formBuilder: CdFormBuilder,
    public actionLabels: ActionLabelsI18n,
    private hostService: HostService,
    private cephfsService: CephfsService
  ) {
    super();
    this.editing = this.router.url.startsWith(`/pool/${URLVerbs.EDIT}`);
    this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.CREATE;
    this.resource = $localize`volume`;
    this.hosts = {
      options: [],
      messages: new SelectMessages({
        empty: $localize`There are no hosts.`,
        filter: $localize`Filter hosts`
      })
    };
    this.createForm();
  }

  private createForm() {
    this.orchService.status().subscribe((status) => {
      this.hasOrchestrator = status.available;
    });
    this.form = this.formBuilder.group({
      name: new FormControl('', {
        validators: [Validators.pattern(/^[.A-Za-z0-9_/-]+$/), Validators.required]
      }),
      placement: ['hosts'],
      hosts: [[]],
      label: [
        null,
        [
          CdValidators.requiredIf({
            placement: 'label',
            unmanaged: false
          })
        ]
      ],
      unmanaged: [false]
    });
  }

  ngOnInit() {
    const hostContext = new CdTableFetchDataContext(() => undefined);
    this.hostService.list(hostContext.toParams(), 'false').subscribe((resp: object[]) => {
      const options: SelectOption[] = [];
      _.forEach(resp, (host: object) => {
        if (_.get(host, 'sources.orchestrator', false)) {
          const option = new SelectOption(false, _.get(host, 'hostname'), '');
          options.push(option);
        }
      });
      this.hosts.options = [...options];
    });
    this.hostService.getLabels().subscribe((resp: string[]) => {
      this.labels = resp;
    });
    this.orchStatus$ = this.orchService.status();
  }

  searchLabels = (text$: Observable<string>) => {
    return merge(
      text$.pipe(debounceTime(200), distinctUntilChanged()),
      this.labelFocus,
      this.labelClick.pipe(filter(() => !this.typeahead.isPopupOpen()))
    ).pipe(
      map((value) =>
        this.labels
          .filter((label: string) => label.toLowerCase().indexOf(value.toLowerCase()) > -1)
          .slice(0, 10)
      )
    );
  };

  submit() {
    let values = this.form.getRawValue();
    const serviceSpec: object = {
      placement: {},
      unmanaged: values['unmanaged']
    };
    switch (values['placement']) {
      case 'hosts':
        if (values['hosts'].length > 0) {
          serviceSpec['placement']['hosts'] = values['hosts'];
        }
        break;
      case 'label':
        serviceSpec['placement']['label'] = values['label'];
        break;
    }

    const volumeName = this.form.get('name').value;
    const self = this;
    let taskUrl = `cephfs/${URLVerbs.CREATE}`;
    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, {
          volumeName: volumeName
        }),
        call: this.cephfsService.create(this.form.get('name').value, serviceSpec)
      })
      .subscribe({
        error() {
          self.form.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.router.navigate(['cephfs']);
        }
      });
  }
}
