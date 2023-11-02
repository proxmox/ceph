import { AfterViewInit, Component, Input, OnChanges, ViewChild } from '@angular/core';

import { CssHelper } from '~/app/shared/classes/css-helper';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { DimlessBinaryPerSecondPipe } from '~/app/shared/pipes/dimless-binary-per-second.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { BaseChartDirective, PluginServiceGlobalRegistrationAndOptions } from 'ng2-charts';
import { DimlessPipe } from '~/app/shared/pipes/dimless.pipe';
import { NumberFormatterService } from '~/app/shared/services/number-formatter.service';

@Component({
  selector: 'cd-dashboard-area-chart',
  templateUrl: './dashboard-area-chart.component.html',
  styleUrls: ['./dashboard-area-chart.component.scss']
})
export class DashboardAreaChartComponent implements OnChanges, AfterViewInit {
  @ViewChild(BaseChartDirective) chart: BaseChartDirective;

  @Input()
  chartTitle: string;
  @Input()
  maxValue?: number;
  @Input()
  dataUnits: string;
  @Input()
  data: Array<[number, string]>;
  @Input()
  data2?: Array<[number, string]>;
  @Input()
  label: string;
  @Input()
  label2?: string;
  @Input()
  decimals?: number = 1;

  currentDataUnits: string;
  currentData: number;
  currentDataUnits2?: string;
  currentData2?: number;

  chartDataUnits: string;

  chartData: any = {
    dataset: [
      {
        label: '',
        data: [{ x: 0, y: 0 }],
        tension: 0,
        pointBackgroundColor: this.cssHelper.propertyValue('chart-color-strong-blue'),
        backgroundColor: this.cssHelper.propertyValue('chart-color-translucent-blue'),
        borderColor: this.cssHelper.propertyValue('chart-color-strong-blue')
      },
      {
        label: '',
        data: [],
        tension: 0,
        pointBackgroundColor: this.cssHelper.propertyValue('chart-color-orange'),
        backgroundColor: this.cssHelper.propertyValue('chart-color-yellow'),
        borderColor: this.cssHelper.propertyValue('chart-color-orange')
      }
    ]
  };

  options: any = {
    responsive: true,
    maintainAspectRatio: false,
    elements: {
      point: {
        radius: 0
      }
    },
    legend: {
      display: false
    },
    tooltips: {
      intersect: false,
      displayColors: true,
      backgroundColor: this.cssHelper.propertyValue('chart-color-tooltip-background'),
      callbacks: {
        title: function (tooltipItem: any): any {
          return tooltipItem[0].xLabel;
        }
      }
    },
    hover: {
      intersect: false
    },
    scales: {
      xAxes: [
        {
          display: false,
          type: 'time',
          gridLines: {
            display: false
          },
          time: {
            tooltipFormat: 'YYYY/MM/DD hh:mm:ss'
          }
        }
      ],
      yAxes: [
        {
          afterFit: (scaleInstance: any) => (scaleInstance.width = 100),
          gridLines: {
            display: false
          },
          ticks: {
            beginAtZero: true,
            maxTicksLimit: 3,
            callback: (value: any) => {
              if (value === 0) {
                return null;
              }
              return this.convertUnits(value);
            }
          }
        }
      ]
    },
    plugins: {
      borderArea: true,
      chartAreaBorder: {
        borderColor: this.cssHelper.propertyValue('chart-color-slight-dark-gray'),
        borderWidth: 2
      }
    }
  };

  public chartAreaBorderPlugin: PluginServiceGlobalRegistrationAndOptions[] = [
    {
      beforeDraw(chart: Chart) {
        if (!chart.options.plugins.borderArea) {
          return;
        }
        const {
          ctx,
          chartArea: { left, top, right, bottom }
        } = chart;
        ctx.save();
        ctx.strokeStyle = chart.options.plugins.chartAreaBorder.borderColor;
        ctx.lineWidth = chart.options.plugins.chartAreaBorder.borderWidth;
        ctx.setLineDash(chart.options.plugins.chartAreaBorder.borderDash || []);
        ctx.lineDashOffset = chart.options.plugins.chartAreaBorder.borderDashOffset;
        ctx.strokeRect(left, top, right - left - 1, bottom);
        ctx.restore();
      }
    }
  ];

  constructor(
    private cssHelper: CssHelper,
    private dimlessBinary: DimlessBinaryPipe,
    private dimlessBinaryPerSecond: DimlessBinaryPerSecondPipe,
    private dimlessPipe: DimlessPipe,
    private formatter: FormatterService,
    private numberFormatter: NumberFormatterService
  ) {}

  ngOnChanges(): void {
    this.updateChartData();
  }

  ngAfterViewInit(): void {
    this.updateChartData();
  }

  private updateChartData(): void {
    if (this.data) {
      this.setChartTicks();
      this.chartData.dataset[0].data = this.formatData(this.data);
      this.chartData.dataset[0].label = this.label;
      [this.currentData, this.currentDataUnits] = this.convertUnits(
        this.data[this.data.length - 1][1]
      ).split(' ');
    }
    if (this.data2) {
      this.chartData.dataset[1].data = this.formatData(this.data2);
      this.chartData.dataset[1].label = this.label2;
      [this.currentData2, this.currentDataUnits2] = this.convertUnits(
        this.data2[this.data2.length - 1][1]
      ).split(' ');
    }
    if (this.chart) {
      this.chart.chart.update();
    }
  }

  private formatData(array: Array<any>): any {
    let formattedData = {};
    formattedData = array.map((data: any) => ({
      x: data[0] * 1000,
      y: Number(this.convertToChartDataUnits(data[1]).replace(/[^\d,.]+/g, ''))
    }));
    return formattedData;
  }

  private convertToChartDataUnits(data: any): any {
    let dataWithUnits: string = '';
    if (this.chartDataUnits) {
      if (this.dataUnits === 'B') {
        dataWithUnits = this.numberFormatter.formatBytesFromTo(
          data,
          this.dataUnits,
          this.chartDataUnits
        );
      } else if (this.dataUnits === 'B/s') {
        dataWithUnits = this.numberFormatter.formatBytesPerSecondFromTo(
          data,
          this.dataUnits,
          this.chartDataUnits
        );
      } else if (this.dataUnits === 'ms') {
        dataWithUnits = this.numberFormatter.formatSecondsFromTo(
          data,
          this.dataUnits,
          this.chartDataUnits,
          this.decimals
        );
      } else {
        dataWithUnits = this.numberFormatter.formatUnitlessFromTo(
          data,
          this.dataUnits,
          this.chartDataUnits
        );
      }
    }
    return dataWithUnits;
  }

  private convertUnits(data: any): any {
    let dataWithUnits: string = '';
    if (this.dataUnits === 'B') {
      dataWithUnits = this.dimlessBinary.transform(data);
    } else if (this.dataUnits === 'B/s') {
      dataWithUnits = this.dimlessBinaryPerSecond.transform(data);
    } else if (this.dataUnits === 'ms') {
      dataWithUnits = this.formatter.format_number(data, 1000, ['ms', 's'], this.decimals);
    } else {
      dataWithUnits = this.dimlessPipe.transform(data);
    }
    return dataWithUnits;
  }

  private setChartTicks() {
    if (!this.chart) {
      return;
    }

    let maxValue = 0;
    let maxValueDataUnits = '';
    let extraRoom = 1.2;

    if (this.maxValue) {
      extraRoom = 1.0;
      [maxValue, maxValueDataUnits] = this.convertUnits(this.maxValue).split(' ');
    } else if (this.data) {
      extraRoom = 1.2;
      let maxValueData = Math.max(...this.data.map((values: any) => values[1]));
      if (this.data2) {
        let maxValueData2 = Math.max(...this.data2.map((values: any) => values[1]));
        maxValue = Math.max(maxValueData, maxValueData2);
      } else {
        maxValue = maxValueData;
      }
      [maxValue, maxValueDataUnits] = this.convertUnits(maxValue).split(' ');
    }

    const yAxesTicks = this.chart.chart.options.scales.yAxes[0].ticks;
    yAxesTicks.suggestedMax = maxValue * extraRoom;
    yAxesTicks.suggestedMin = 0;
    yAxesTicks.stepSize = Number((yAxesTicks.suggestedMax / 2).toFixed(0));
    yAxesTicks.callback = (value: any) => {
      if (value === 0) {
        return null;
      }
      if (!maxValueDataUnits) {
        return `${value}`;
      }
      return `${value} ${maxValueDataUnits}`;
    };
    this.chartDataUnits = maxValueDataUnits || '';
    this.chart.chart.update();
  }
}
