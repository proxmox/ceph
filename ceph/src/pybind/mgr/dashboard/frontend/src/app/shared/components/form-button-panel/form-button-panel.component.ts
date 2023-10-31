import { Location } from '@angular/common';
import { Component, EventEmitter, Input, Output, ViewChild } from '@angular/core';
import { FormGroup, NgForm } from '@angular/forms';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { ModalService } from '~/app/shared/services/modal.service';
import { SubmitButtonComponent } from '../submit-button/submit-button.component';

@Component({
  selector: 'cd-form-button-panel',
  templateUrl: './form-button-panel.component.html',
  styleUrls: ['./form-button-panel.component.scss']
})
export class FormButtonPanelComponent {
  @ViewChild(SubmitButtonComponent)
  submitButton: SubmitButtonComponent;

  @Output()
  submitActionEvent = new EventEmitter();
  @Output()
  backActionEvent = new EventEmitter();

  @Input()
  form: FormGroup | NgForm;
  @Input()
  showSubmit = true;
  @Input()
  showCancel = true;
  @Input()
  wrappingClass = '';
  @Input()
  btnClass = '';
  @Input()
  submitText: string = this.actionLabels.CREATE;
  @Input()
  cancelText: string = this.actionLabels.CANCEL;
  @Input()
  disabled = false;

  constructor(
    private location: Location,
    private actionLabels: ActionLabelsI18n,
    private modalService: ModalService
  ) {}

  submitAction() {
    this.submitActionEvent.emit();
  }

  backAction() {
    if (this.backActionEvent.observers.length === 0) {
      if (this.modalService.hasOpenModals()) {
        this.modalService.dismissAll();
      } else {
        this.location.back();
      }
    } else {
      this.backActionEvent.emit();
    }
  }
}
