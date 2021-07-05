import { ChangeDetectorRef, Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalService } from 'ngx-bootstrap/modal';
import { forkJoin as observableForkJoin, Observable, Subscriber } from 'rxjs';

import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permission } from '../../../shared/models/permissions';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { URLBuilderService } from '../../../shared/services/url-builder.service';

const BASE_URL = 'rgw/bucket';

@Component({
  selector: 'cd-rgw-bucket-list',
  templateUrl: './rgw-bucket-list.component.html',
  styleUrls: ['./rgw-bucket-list.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class RgwBucketListComponent implements OnInit {
  @ViewChild(TableComponent)
  table: TableComponent;
  @ViewChild('bucketSizeTpl')
  bucketSizeTpl: TemplateRef<any>;
  @ViewChild('bucketObjectTpl')
  bucketObjectTpl: TemplateRef<any>;

  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[] = [];
  buckets: object[] = [];
  selection: CdTableSelection = new CdTableSelection();

  constructor(
    private authStorageService: AuthStorageService,
    private dimlessBinaryPipe: DimlessBinaryPipe,
    private dimlessPipe: DimlessPipe,
    private rgwBucketService: RgwBucketService,
    private bsModalService: BsModalService,
    private i18n: I18n,
    private urlBuilder: URLBuilderService,
    public actionLabels: ActionLabelsI18n,
    private changeDetectorRef: ChangeDetectorRef
  ) {
    this.permission = this.authStorageService.getPermissions().rgw;
    const getBucketUri = () =>
      this.selection.first() && `${encodeURIComponent(this.selection.first().bid)}`;
    const addAction: CdTableAction = {
      permission: 'create',
      icon: 'fa-plus',
      routerLink: () => this.urlBuilder.getCreate(),
      name: this.actionLabels.CREATE
    };
    const editAction: CdTableAction = {
      permission: 'update',
      icon: 'fa-pencil',
      routerLink: () => this.urlBuilder.getEdit(getBucketUri()),
      name: this.actionLabels.EDIT
    };
    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: 'fa-times',
      click: () => this.deleteAction(),
      name: this.actionLabels.DELETE
    };
    this.tableActions = [addAction, editAction, deleteAction];
  }

  ngOnInit() {
    this.columns = [
      {
        name: this.i18n('Name'),
        prop: 'bid',
        flexGrow: 2
      },
      {
        name: this.i18n('Owner'),
        prop: 'owner',
        flexGrow: 3
      },
      {
        name: this.i18n('Used Capacity'),
        prop: 'bucket_size',
        flexGrow: 0.5,
        pipe: this.dimlessBinaryPipe
      },
      {
        name: this.i18n('Capacity Limit %'),
        prop: 'size_usage',
        cellTemplate: this.bucketSizeTpl,
        flexGrow: 1
      },
      {
        name: this.i18n('Objects'),
        prop: 'num_objects',
        flexGrow: 0.5,
        pipe: this.dimlessPipe
      },
      {
        name: this.i18n('Object Limit %'),
        prop: 'object_usage',
        cellTemplate: this.bucketObjectTpl,
        flexGrow: 1
      }
    ];
  }

  transformBucketData() {
    _.forEach(this.buckets, (bucketKey) => {
      const maxBucketSize = bucketKey['bucket_quota']['max_size'];
      const maxBucketObjects = bucketKey['bucket_quota']['max_objects'];
      bucketKey['bucket_size'] = 0;
      bucketKey['num_objects'] = 0;
      if (!_.isEmpty(bucketKey['usage'])) {
        bucketKey['bucket_size'] = bucketKey['usage']['rgw.main']['size_actual'];
        bucketKey['num_objects'] = bucketKey['usage']['rgw.main']['num_objects'];
      }
      bucketKey['size_usage'] =
        maxBucketSize > 0 ? bucketKey['bucket_size'] / maxBucketSize : undefined;
      bucketKey['object_usage'] =
        maxBucketObjects > 0 ? bucketKey['num_objects'] / maxBucketObjects : undefined;
    });
  }

  getBucketList(context: CdTableFetchDataContext) {
    this.rgwBucketService.list().subscribe(
      (resp: object[]) => {
        this.buckets = resp;
        this.transformBucketData();
        this.changeDetectorRef.detectChanges();
      },
      () => {
        context.error();
      }
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteAction() {
    this.bsModalService.show(CriticalConfirmationModalComponent, {
      initialState: {
        itemDescription: this.selection.hasSingleSelection
          ? this.i18n('bucket')
          : this.i18n('buckets'),
        itemNames: this.selection.selected.map((bucket: any) => bucket['bid']),
        submitActionObservable: () => {
          return new Observable((observer: Subscriber<any>) => {
            // Delete all selected data table rows.
            observableForkJoin(
              this.selection.selected.map((bucket: any) => {
                return this.rgwBucketService.delete(bucket.bid);
              })
            ).subscribe(
              null,
              (error) => {
                // Forward the error to the observer.
                observer.error(error);
                // Reload the data table content because some deletions might
                // have been executed successfully in the meanwhile.
                this.table.refreshBtn();
              },
              () => {
                // Notify the observer that we are done.
                observer.complete();
                // Reload the data table content.
                this.table.refreshBtn();
              }
            );
          });
        }
      }
    });
  }
}
