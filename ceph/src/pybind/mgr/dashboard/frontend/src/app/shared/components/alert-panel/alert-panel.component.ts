import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { I18n } from '@ngx-translate/i18n-polyfill';
import { Icons } from '../../enum/icons.enum';

@Component({
  selector: 'cd-alert-panel',
  templateUrl: './alert-panel.component.html',
  styleUrls: ['./alert-panel.component.scss']
})
export class AlertPanelComponent implements OnInit {
  @Input()
  title = '';
  @Input()
  bootstrapClass = '';
  @Output()
  backAction = new EventEmitter();
  @Input()
  type: 'warning' | 'error' | 'info' | 'success' | 'danger';
  @Input()
  typeIcon: Icons | string;
  @Input()
  size: 'slim' | 'normal' = 'normal';
  @Input()
  showIcon = true;
  @Input()
  showTitle = true;
  @Input()
  dismissible = false;

  /**
   * The event that is triggered when the close button (x) has been
   * pressed.
   */
  @Output()
  dismissed = new EventEmitter();

  icons = Icons;

  constructor(private i18n: I18n) {}

  ngOnInit() {
    switch (this.type) {
      case 'warning':
        this.title = this.title || this.i18n('Warning');
        this.typeIcon = this.typeIcon || Icons.warning;
        this.bootstrapClass = this.bootstrapClass || 'warning';
        break;
      case 'error':
        this.title = this.title || this.i18n('Error');
        this.typeIcon = this.typeIcon || Icons.destroyCircle;
        this.bootstrapClass = this.bootstrapClass || 'danger';
        break;
      case 'info':
        this.title = this.title || this.i18n('Information');
        this.typeIcon = this.typeIcon || Icons.infoCircle;
        this.bootstrapClass = this.bootstrapClass || 'info';
        break;
      case 'success':
        this.title = this.title || this.i18n('Success');
        this.typeIcon = this.typeIcon || Icons.check;
        this.bootstrapClass = this.bootstrapClass || 'success';
        break;
      case 'danger':
        this.title = this.title || this.i18n(`Danger`);
        this.typeIcon = this.typeIcon || Icons.warning;
        this.bootstrapClass = this.bootstrapClass || 'danger';
        break;
    }
  }

  onClose(): void {
    this.dismissed.emit();
  }
}
