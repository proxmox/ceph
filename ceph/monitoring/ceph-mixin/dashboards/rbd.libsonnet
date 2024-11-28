local g = import 'grafonnet/grafana.libsonnet';
local u = import 'utils.libsonnet';

(import 'utils.libsonnet') {
  'rbd-details.json':
    local RbdDetailsPanel(title, formatY1, expr1, expr2, x, y, w, h) =
      $.graphPanelSchema({},
                         title,
                         '',
                         'null as zero',
                         false,
                         formatY1,
                         formatY1,
                         null,
                         null,
                         0,
                         1,
                         '$datasource')
      .addTargets(
        [
          $.addTargetSchema(expr1,
                            '{{pool}} Write'),
          $.addTargetSchema(expr2, '{{pool}} Read'),
        ]
      ) + { type: 'timeseries' } + { fieldConfig: { defaults: { unit: formatY1, custom: { fillOpacity: 8, showPoints: 'never' } } } } + { gridPos: { x: x, y: y, w: w, h: h } };

    $.dashboardSchema(
      'RBD Details',
      'Detailed Performance of RBD Images (IOPS/Throughput/Latency)',
      'YhCYGcuZz',
      'now-1h',
      '30s',
      16,
      $._config.dashboardTags,
      ''
    )
    .addAnnotation(
      $.addAnnotationSchema(
        1,
        '-- Grafana --',
        true,
        true,
        'rgba(0, 211, 255, 1)',
        'Annotations & Alerts',
        'dashboard'
      )
    )
    .addRequired(
      type='grafana', id='grafana', name='Grafana', version='5.3.3'
    )
    .addRequired(
      type='panel', id='graph', name='Graph', version='5.0.0'
    )
    .addTemplate(
      g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate(
      $.addClusterTemplate()
    )
    .addTemplate(
      $.addJobTemplate()
    )
    .addTemplate(
      $.addTemplateSchema('pool',
                          '$datasource',
                          'label_values(pool)',
                          1,
                          false,
                          0,
                          '',
                          '')
    )
    .addTemplate(
      $.addTemplateSchema('image',
                          '$datasource',
                          'label_values(image)',
                          1,
                          false,
                          0,
                          '',
                          '')
    )
    .addPanels([
      RbdDetailsPanel(
        'IOPS',
        'iops',
        'rate(ceph_rbd_write_ops{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval])' % $.matchers()
        ,
        'rate(ceph_rbd_read_ops{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval])' % $.matchers(),
        0,
        0,
        8,
        9
      ),
      RbdDetailsPanel(
        'Throughput',
        'Bps',
        'rate(ceph_rbd_write_bytes{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval])' % $.matchers(),
        'rate(ceph_rbd_read_bytes{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval])' % $.matchers(),
        8,
        0,
        8,
        9
      ),
      RbdDetailsPanel(
        'Average Latency',
        'ns',
        |||
          rate(ceph_rbd_write_latency_sum{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval]) /
            rate(ceph_rbd_write_latency_count{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval])
        ||| % $.matchers(),
        |||
          rate(ceph_rbd_read_latency_sum{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval]) /
            rate(ceph_rbd_read_latency_count{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval])
        ||| % $.matchers(),
        16,
        0,
        8,
        9
      ),
    ]),
  'rbd-overview.json':
    local RbdOverviewPanel(title,
                           formatY1,
                           expr1,
                           expr2,
                           legendFormat1,
                           legendFormat2,
                           x,
                           y,
                           w,
                           h) =
      $.graphPanelSchema({},
                         title,
                         '',
                         'null as zero',
                         false,
                         formatY1,
                         'short',
                         null,
                         null,
                         0,
                         1,
                         '$datasource')
      .addTargets(
        [
          $.addTargetSchema(expr1,
                            legendFormat1),
          $.addTargetSchema(expr2,
                            legendFormat2),
        ]
      ) + { type: 'timeseries' } + { fieldConfig: { defaults: { unit: formatY1, custom: { fillOpacity: 8, showPoints: 'never' } } } } + { gridPos: { x: x, y: y, w: w, h: h } };

    $.dashboardSchema(
      'RBD Overview',
      '',
      '41FrpeUiz',
      'now-1h',
      '30s',
      16,
      $._config.dashboardTags + ['overview'],
      ''
    )
    .addAnnotation(
      $.addAnnotationSchema(
        1,
        '-- Grafana --',
        true,
        true,
        'rgba(0, 211, 255, 1)',
        'Annotations & Alerts',
        'dashboard'
      )
    )
    .addRequired(
      type='grafana', id='grafana', name='Grafana', version='5.4.2'
    )
    .addRequired(
      type='panel', id='graph', name='Graph', version='5.0.0'
    )
    .addRequired(
      type='datasource', id='prometheus', name='Prometheus', version='5.0.0'
    )
    .addRequired(
      type='panel', id='table', name='Table', version='5.0.0'
    )
    .addTemplate(
      g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate(
      $.addClusterTemplate()
    )
    .addTemplate(
      $.addJobTemplate()
    )
    .addPanels([
      RbdOverviewPanel(
        'IOPS',
        'short',
        'round(sum(rate(ceph_rbd_write_ops{%(matchers)s}[$__rate_interval])))' % $.matchers(),
        'round(sum(rate(ceph_rbd_read_ops{%(matchers)s}[$__rate_interval])))' % $.matchers(),
        'Writes',
        'Reads',
        0,
        0,
        8,
        7
      ),
      RbdOverviewPanel(
        'Throughput',
        'Bps',
        'round(sum(rate(ceph_rbd_write_bytes{%(matchers)s}[$__rate_interval])))' % $.matchers(),
        'round(sum(rate(ceph_rbd_read_bytes{%(matchers)s}[$__rate_interval])))' % $.matchers(),
        'Write',
        'Read',
        8,
        0,
        8,
        7
      ),
      RbdOverviewPanel(
        'Average Latency',
        'ns',
        |||
          round(
            sum(rate(ceph_rbd_write_latency_sum{%(matchers)s}[$__rate_interval])) /
              sum(rate(ceph_rbd_write_latency_count{%(matchers)s}[$__rate_interval]))
          )
        ||| % $.matchers(),
        |||
          round(
            sum(rate(ceph_rbd_read_latency_sum{%(matchers)s}[$__rate_interval])) /
              sum(rate(ceph_rbd_read_latency_count{%(matchers)s}[$__rate_interval]))
          )
        ||| % $.matchers(),
        'Write',
        'Read',
        16,
        0,
        8,
        7
      ),

      $.addTableExtended(
        datasource='${datasource}',
        title='Highest IOPS',
        description='RBD per-image IO statistics are disabled by default.\n\nPlease refer to https://docs.ceph.com/en/latest/mgr/prometheus/#rbd-io-statistics for information about how to enable those optionally.',
        gridPosition={ h: 7, w: 8, x: 0, y: 7 },
        options={
          footer: {
            fields: '',
            reducer: ['sum'],
            countRows: false,
            enablePagination: false,
            show: false,
          },
          frameIndex: 1,
          showHeader: true,
        },
        custom={ align: 'null', cellOptions: { type: 'auto' }, filterable: true, inspect: false },
        thresholds={
          mode: 'absolute',
          steps: [
            { color: 'green', value: null },
            { color: 'red', value: 80 },
          ],
        },
        overrides=[
          {
            matcher: { id: 'byName', options: 'pool' },
            properties: [
              { id: 'displayName', value: 'Pool' },
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
              { id: 'custom.align', value: null },
            ],
          },
          {
            matcher: { id: 'byName', options: 'image' },
            properties: [
              { id: 'displayName', value: 'Image' },
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
              { id: 'custom.align', value: null },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value' },
            properties: [
              { id: 'displayName', value: 'IOPS' },
              { id: 'unit', value: 'iops' },
              { id: 'decimals', value: 2 },
              { id: 'custom.align', value: null },
            ],
          },
        ],
        pluginVersion='10.4.0'
      )
      .addTransformations([
        {
          id: 'merge',
          options: { reducers: [] },
        },
      ]).addTarget(
        $.addTargetSchema(
          |||
            topk(10,
              (
                sort((
                  rate(ceph_rbd_write_ops{%(matchers)s}[$__rate_interval]) +
                    on (image, pool, namespace) rate(ceph_rbd_read_ops{%(matchers)s}[$__rate_interval])
                ))
              )
            )
          ||| % $.matchers(),
          '',
          'table',
          1,
          true
        )
      ),

      $.addTableExtended(
        datasource='${datasource}',
        title='Highest Throughput',
        description='RBD per-image IO statistics are disabled by default.\n\nPlease refer to https://docs.ceph.com/en/latest/mgr/prometheus/#rbd-io-statistics for information about how to enable those optionally.',
        gridPosition={ h: 7, w: 8, x: 8, y: 7 },
        options={
          footer: {
            fields: '',
            reducer: ['sum'],
            countRows: false,
            enablePagination: false,
            show: false,
          },
          frameIndex: 1,
          showHeader: true,
        },
        custom={ align: 'null', cellOptions: { type: 'auto' }, filterable: true, inspect: false },
        thresholds={
          mode: 'absolute',
          steps: [
            { color: 'green', value: null },
            { color: 'red', value: 80 },
          ],
        },
        overrides=[
          {
            matcher: { id: 'byName', options: 'pool' },
            properties: [
              { id: 'displayName', value: 'Pool' },
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
              { id: 'custom.align', value: null },
            ],
          },
          {
            matcher: { id: 'byName', options: 'image' },
            properties: [
              { id: 'displayName', value: 'Image' },
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
              { id: 'custom.align', value: null },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value' },
            properties: [
              { id: 'displayName', value: 'Throughput' },
              { id: 'unit', value: 'Bps' },
              { id: 'decimals', value: 2 },
              { id: 'custom.align', value: null },
            ],
          },
        ],
        pluginVersion='10.4.0'
      )
      .addTransformations([
        {
          id: 'merge',
          options: { reducers: [] },
        },
      ]).addTarget(
        $.addTargetSchema(
          |||
            topk(10,
              sort(
                sum(
                  rate(ceph_rbd_read_bytes{%(matchers)s}[$__rate_interval]) +
                    rate(ceph_rbd_write_bytes{%(matchers)s}[$__rate_interval])
                ) by (pool, image, namespace)
              )
            )
          ||| % $.matchers(),
          '',
          'table',
          1,
          true
        )
      ),

      $.addTableExtended(
        datasource='${datasource}',
        title='Highest Latency',
        description='RBD per-image IO statistics are disabled by default.\n\nPlease refer to https://docs.ceph.com/en/latest/mgr/prometheus/#rbd-io-statistics for information about how to enable those optionally.',
        gridPosition={ h: 7, w: 8, x: 16, y: 7 },
        options={
          footer: {
            fields: '',
            reducer: ['sum'],
            countRows: false,
            enablePagination: false,
            show: false,
          },
          frameIndex: 1,
          showHeader: true,
        },
        custom={ align: 'null', cellOptions: { type: 'auto' }, filterable: true, inspect: false },
        thresholds={
          mode: 'absolute',
          steps: [
            { color: 'green', value: null },
            { color: 'red', value: 80 },
          ],
        },
        overrides=[
          {
            matcher: { id: 'byName', options: 'pool' },
            properties: [
              { id: 'displayName', value: 'Pool' },
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
              { id: 'custom.align', value: null },
            ],
          },
          {
            matcher: { id: 'byName', options: 'image' },
            properties: [
              { id: 'displayName', value: 'Image' },
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
              { id: 'custom.align', value: null },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value' },
            properties: [
              { id: 'displayName', value: 'Latency' },
              { id: 'unit', value: 'ns' },
              { id: 'decimals', value: 2 },
              { id: 'custom.align', value: null },
            ],
          },
        ],
        pluginVersion='10.4.0'
      )
      .addTransformations([
        {
          id: 'merge',
          options: { reducers: [] },
        },
      ]).addTarget(
        $.addTargetSchema(
          |||
            topk(10,
              sum(
                rate(ceph_rbd_write_latency_sum{%(matchers)s}[$__rate_interval]) /
                  clamp_min(rate(ceph_rbd_write_latency_count{%(matchers)s}[$__rate_interval]), 1) +
                  rate(ceph_rbd_read_latency_sum{%(matchers)s}[$__rate_interval]) /
                  clamp_min(rate(ceph_rbd_read_latency_count{%(matchers)s}[$__rate_interval]), 1)
              ) by (pool, image, namespace)
            )
          ||| % $.matchers(),
          '',
          'table',
          1,
          true
        )
      ),
    ]),
}
