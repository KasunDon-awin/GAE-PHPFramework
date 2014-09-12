<?php

namespace Lib\Provider;

use Silex\Application;
use Silex\ServiceProviderInterface;
use Symfony\Component\Yaml\Yaml;


class YamlConfigProvider implements ServiceProviderInterface
{
    /**
     * @var string 
     */
    protected $file;
    
    /**
     * constructor
     * 
     * @param string $file
     */
    public function __construct($file) {
        $this->file = $file;
    }
    
     /**
     * Adding config from sub import resources
     * 
     * @param type $config
     * @param type $app
     */
    public function addSubImports(&$config, $app) {
        foreach ($config as $key => $value) {
            if ($key === 'imports') {
                foreach ($value as $resource) {
                    $path = pathinfo($this->file);
                    $ymlConfig = new YamlConfigServiceProvider($path['dirname'] . "/" . $resource['resource']);
                    $ymlConfig->register($app);
                }
                unset($config['imports']);
            }
        }
    }

    /**
     * 
     * @param \Silex\Application $app
     */
    public function register(Application $app) {
        $config = Yaml::parse(file_get_contents($this->file));

        if (is_array($config)) {
            $this->addSubImports($config, $app);
            if (isset($app['config']) && is_array($app['config'])) {
                $app['config'] = array_replace_recursive($app['config'], $config);
            } else {
                $app['config'] = $config;
            }
        }

    }
   
    public function boot(Application $app) {
    }
}