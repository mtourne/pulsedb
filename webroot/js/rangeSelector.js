var RangeSelector = function(id, ranges){
  var min   =   1;
  var hour  =  60*min;
  var day   =  24*hour;
  var month =  30*day;

  var self = this;
  self.id = id;
  self.container = $('#'+id);

  self.ranges = ranges || {
    "1y":   12*month,
    "6m":    6*month,
    "3m":    3*month,
    "1m":    1*month,
    "1w":    7*day,
    "1d":      day,
    "12h":  12*hour,
    "1h":      hour,
    "15m":  15*min,
    "5m":    5*min,
    "fresh":   min };

  self.btn_class = 'range-selector';
  self.buttons = function(){
    return $('.'+self.btn_class);
  };

  self.make_range_btn = function(label, step){
    return $('<button></button>')
    .addClass(self.btn_class)
    .text(label)
    .data('step', step)
    .on('click', function(){
      var btn = $(this);
      self.buttons().removeClass('active');
      btn.addClass('active');
      window.history.replaceState("", "", location.pathname+"#"+step);
      window.Graphic.request(btn.data('step'));
    });
  };

  self.add_button = function(label, step){
    var btn = self.make_range_btn(label, step);
    self.container.append(btn);
  };

  self.render = function(){
    if (self.container.length == 0)
      return;
    for (key in self.ranges){
      var label = key;
      var step = self.ranges[key];
      var btn = self.make_range_btn(label, step);
      self.container.append(btn);
    }
  };

  self.toggle = function(show){
    self.container.toggle(show);
  };

  self.enable = function(){
    clearTimeout(self.enableTimer);
    self.buttons().removeAttr('disabled');
  };

  self.disable = function(releaseTime){
    clearTimeout(self.enableTimer);
    self.buttons().attr('disabled', 'disabled');

    if (releaseTime)
      self.enableTimer = setTimeout(self.enable, 5000);
  }

  self.render();
  return self;
};
