Vagrant::Config.run do |config|
  config.vm.define :ubuntu do |conf|
    conf.vm.box = "precise64"
    conf.vm.box_url = "https://s3-us-west-2.amazonaws.com/squishy.vagrant-boxes/precise64_squishy_2013-02-09.box"
    conf.vm.forward_port 6800, 8056
    conf.vm.forward_port 80, 8054
    conf.vm.provision :shell, :path => "deploy/ubuntu.sh"   
  end

end

