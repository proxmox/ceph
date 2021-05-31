import { Component, Input, NgZone, OnChanges, OnDestroy, OnInit } from '@angular/core';

import { Subscription, timer as observableTimer } from 'rxjs';

import { OrchestratorService } from '../../../shared/api/orchestrator.service';
import { Icons } from '../../../shared/enum/icons.enum';
import { InventoryDevice } from './inventory-devices/inventory-device.model';

@Component({
  selector: 'cd-inventory',
  templateUrl: './inventory.component.html',
  styleUrls: ['./inventory.component.scss']
})
export class InventoryComponent implements OnChanges, OnInit, OnDestroy {
  // Display inventory page only for this hostname, ignore to display all.
  @Input() hostname?: string;

  private reloadSubscriber: Subscription;
  private reloadInterval = 5000;
  private firstRefresh = true;

  icons = Icons;

  hasOrchestrator = false;
  showDocPanel = false;

  devices: Array<InventoryDevice> = [];

  constructor(private orchService: OrchestratorService, private ngZone: NgZone) {}

  ngOnInit() {
    this.orchService.status().subscribe((status) => {
      this.hasOrchestrator = status.available;
      this.showDocPanel = !status.available;
      if (status.available) {
        // Create a timer to get cached inventory from the orchestrator.
        // Do not ask the orchestrator frequently to refresh its cache data because it's expensive.
        this.ngZone.runOutsideAngular(() => {
          // start after first pass because the embedded table calls refresh at init.
          this.reloadSubscriber = observableTimer(
            this.reloadInterval,
            this.reloadInterval
          ).subscribe(() => {
            this.ngZone.run(() => {
              this.getInventory(false);
            });
          });
        });
      }
    });
  }

  ngOnDestroy() {
    if (this.reloadSubscriber) {
      this.reloadSubscriber.unsubscribe();
    }
  }

  ngOnChanges() {
    if (this.hasOrchestrator) {
      this.devices = [];
      this.getInventory(false);
    }
  }

  getInventory(refresh: boolean) {
    if (this.hostname === '') {
      return;
    }
    this.orchService.inventoryDeviceList(this.hostname, refresh).subscribe(
      (devices: InventoryDevice[]) => {
        this.devices = devices;
      },
      () => {
        this.devices = [];
      }
    );
  }

  refresh() {
    // Make the first reload (triggered by table) use cached data, and
    // the remaining reloads (triggered by users) ask orchestrator to refresh inventory.
    this.getInventory(!this.firstRefresh);
    this.firstRefresh = false;
  }
}
