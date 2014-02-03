(function() {
  function tooltipFormatter() {
    var points = this.points || [{point:this.point}];
    // Always print small timestamp
    var t = '<span style="font-size:xx-small">' + Highcharts.dateFormat('%Y-%m-%d %H:%M:%S', this.x) + '</span><br/>';
    $.each(points, function(i, item) {
      var point = item.point, series = point.series;
      var pointDesc = describePoint(point);
      // Print description for each selected (series, point) pair
      t += '<br><span style="color:'+series.color+'">'+series.name+'</span>: <b>'+pointDesc+'</b>';
    });
    return t;
  };

  function describePoint(point) {
    return point.y.toFixed(0);
  };

  function graphicArgs(ID, Options){
    // Reset zero options to empty object (for cases when we get null, undefined, [] or so)
    if (! Options) Options = {};

    // Display options
    var chartOptions = {renderTo: ID,
                        animation: false,
                        type: 'spline'};

    var dataLabels = {
      enabled:true,
      formatter:function() {return this.point.title;},
      y:-12,
      borderRadius:3,
      borderColor:"black",
      borderWidth:1,
      backgroundColor:"#bebebe"
    };


    // ordinal = false makes point interval be proportional to actual time difference
    var xAxis = {ordinal: Options.ordinal,
                 type: 'datetime'};

    var yAxis = { min: 0 };

   // if (Options.lines) yAxis.plotLines = genPlotLines(Options.lines);

    // Make floating title if needed
    var title = undefined;
    if (Options.title)
      title = {text: Options.title,
               floating: true};

    var legend = {enabled: true,
                  align: "center",
                  floating: true,
                  y: -20};

    var plotOptions = {series: {marker: {enabled: false},
                                lineWidth: 2,
                                states: {hover: {lineWidth: 2}}}};

    var args = {
      chart: chartOptions,
      scrollbar: {enabled: false},
      xAxis: xAxis,
      yAxis: yAxis,
      title: title,
      legend: legend,
      credits: {enabled: false},
      tooltip: {formatter: tooltipFormatter},
      plotOptions: plotOptions
    };
    return args;
  };

  function emptyGraphicIfNeed(ID){
    var chart = window.graphics[ID];
    if (chart)
      return;
    var args = graphicArgs(ID, {});
    chart = new Highcharts.Chart(args);
    chart.stopWebsocket = function(){};
    window.graphics[ID] = chart;
  };

  function renderGraphic(ID, Options, Data) {
    // Destroy existing chart with same ID if any
    var chart = window.graphics[ID];
    if (chart) {
      chart.hideLoading();
      chart.destroy();
    }

    // auto-add marks series
    var haveMarks = false;
    $.each(Data, function(i, s) {if (s.name == "$marks") haveMarks = true; });

    // Set series ids for use with Chart.get(id)
    $.each(Data, function(i, s) {
      s.id = "series-" + s.name;
      if (s.name == "$marks") {
        s.type = "spline";
        s.color = "#40b040";
        s.dataLabels = dataLabels;
        setMarkLabels(s.data);
      };
    });


    var args = graphicArgs(ID, Options);

    args.series = Data;

    chart = new Highcharts.Chart(args);

    chart.series.forEach(function(s){
      s.pointCountToShift = 100;
    });

    chart.stopWebsocket = function(){
      if (chart.redrawTimer){
        clearTimeout(chart.redrawTimer);
        delete chart.redrawTimer;
      }
      if (chart.websocket){
        chart.websocket.close();
        delete chart.websocket;
      }
    };


    window.graphics[ID] = chart;

    return chart;
  };

  function requestHttpGraphic(ID, Embed, subpath, step) {
    var chart = window.graphics[ID];
    if (chart){
      chart.stopWebsocket();
      window.rangeSelector.disable(5000);
      chart.showLoading();
    }


    var uri = "http://" + window.location.host + (subpath || "/history");
    var request = {url: uri, type: "GET"};
    if (step)
      request.headers = {'X-Use-Step': step };

    $.ajax(request)
     .done(function(data){
       if (data.init){
         window.rangeSelector.enable();
         renderGraphic(ID, data.options, data.data);
         window.Graphic.connected();
       } else {
         window.Graphic.reconnect("http: no 'init' found");
       }
     })
     .fail(function(jqXHR, status) {
       window.Graphic.reconnect("http: error "+ status);
     });
  }

  function requestWsGraphic(ID, Embed, subpath, step) {
    var chart = window.graphics[ID];
    if (chart){
      chart.stopWebsocket();
      window.rangeSelector.disable(5000);
      chart.showLoading();
    }

    // Generate WS uri based on current host
    var uri = "ws://" + window.location.host + (subpath || "/graphic");
    var s = new WebSocket(uri);

    // Send MFA jyst after open
    s.onopen = function(evt) {
      var params = {embed: Embed};
      if (step)
        params.use_step = step;
      s.send(JSON.stringify(params));
    };

    // Accept message
    s.onmessage = function(evt) {
      var data = JSON.parse(evt.data);
      if(!document.getElementById(ID)) {
        s.close();
        return;
      }
      if (data.init) {
        window.rangeSelector.enable();
        // Initial render
        var graph = renderGraphic(ID, data.options, data.data);
        // Store websocket for event handlers
        graph.websocket = s;
        // To improve viewing speed we trigger redraw externally, not on every data packet
        startRedraw(ID, graph, 250);
        window.Graphic.connected();
      } else if (data.set) {
        setGraphicData(ID, data);
      } else {
        updateGraphic(ID, data, data.shift);
      };
    };

    s.onclose = function(evt) {
      var chart = window.graphics[ID];
      if (chart) {
        chart.websocket && delete chart.websocket;
      }
      window.Graphic.reconnect("ws: lost connection");
    };
  };

  function updateGraphic(ID, Data, shift) {
    var chart = window.graphics[ID];
    for (var s in Data) {
      var series = chart.get("series-" + s);
      if (s == "$marks") {
        updateMarks(series, Data[s], shift)
      }
      else if(series) updateSeries(series, Data[s], shift);
    };
  };

  function updateSeries(series, points, shift) {
    var count = points.length;
    var canShift = series.data.length > series.pointCountToShift;
    for (var i = 0; i < count; i++) {
      series.addPoint(points[i], false, shift && canShift);
    };
  };

  function setMarkLabels(marks) {
    return $.map(marks, function(m) {
      if (m.title == undefined) {}
      else if (m.title == null) m.dataLabels = {enabled:false};
      else m.dataLabels = {enabled:true};
      return m.mark_id;
    });
  };

  function updateMarks(series, marks) {
    var markIds = setMarkLabels(marks);
    var sData = series.data;
    var dataLen = sData.length;
    // scan old events, when ids compare equal, update
    for (var i = dataLen - 1; i >= 0 && markIds.length > 0; i --) {
      var cur = sData[i];
      var j = markIds.indexOf(cur.mark_id);
      // Positive j means sData[i] should be updated with marks[j]
      if (j >= 0) {
        markIds.splice(j, 1);
        var mark = marks.splice(j, 1)[0];
        cur.update(mark);
      };
    };
    // Now marks contain only new points
    $.map(marks, function(m) {series.addPoint(m)});
  };

  function setGraphicData(ID, Data) {
    var chart = window.graphics[ID];
    for (var s in Data) {
      if (Data[s] == true) continue; // Flag
      var series = chart.get("series-" + s);
      if(series && series.setData) series.setData(Data[s], false);
    };
    chart.redraw();
  };

  function autoHeight(elt, ratio) {
    $(elt).height(elt.clientWidth/ratio);
  };


  function startRedraw(Id, graph, interval) {
    var chart = window.graphics[Id];
    chart.redrawTimer = setTimeout(function() {
      graph.redraw();
      if (graph.websocket) startRedraw(Id, graph, interval);
    }, interval);
  };


  var self = {
    init: init,
    autoHeight: autoHeight,
    render: renderGraphic,
    ws_request: requestWsGraphic,
    request: dispatchRequest,
    connectionFailures: 0,
    reconnect: processFailure,
    connected: processConnect
  };


  function init(config){
    self.config = config || {};
    self.config.realtime_threshold |= 15;
    emptyGraphicIfNeed(self.config.container);
    window.rangeSelector = new RangeSelector(self.config.range_selector);
  };

  function dispatchRequest(step, protocol){
    self.currentStep = step;
    if (step <= self.config.realtime_threshold && (protocol === 'ws' || !protocol)){
      self.currentProtocol = 'ws';
      requestWsGraphic(self.config.container, self.config.embed, self.config.ws_path, step);
    }else{
      self.currentProtocol = 'http';
      requestHttpGraphic(self.config.container, self.config.embed, self.config.http_path, step);
    }
  }

  function processFailure(errorMsg){
    self.connectionFailures++;
    clearTimeout(self.reconnectTimer);

    var delay = self.connectionFailures % 10;
    var step = self.currentStep;

    // protocol is 'ws'   and failures  < threshold -> reconnect using websocket
    // protocol is 'ws'   and failures >= threshold -> reconnect using http
    // protocol is 'http'                           -> reconnect using http
    var protocol = self.currentProtocol === 'ws'
                && self.connectionFailures < self.config.failure_threshold
                 ? 'ws'
                 : 'http';
    console.log("Next try after " + delay + " seconds");
    self.reconnectTimer = setTimeout(function(){
      self.request(step, protocol)
    }, 1000*delay);
  }

  function processConnect(){
    window.rangeSelector.setUrlData(self.currentStep, self.currentProtocol);
  }

  window.Graphic = self;
  window.graphics = {};
})();
