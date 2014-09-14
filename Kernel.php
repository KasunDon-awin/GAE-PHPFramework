<?php

require_once __DIR__ . '/Autoload.php';

use Lib\Datastore\Service;

$app = new Silex\Application();

$app['debug'] = true;

//registering controller service provider
$app->register(new Silex\Provider\ServiceControllerServiceProvider());

//registering views
$app->register(new Silex\Provider\TwigServiceProvider(), array(
    'twig.path' => __DIR__.'/views',
));

$app->register(new Lib\Provider\YamlConfigProvider('config.yml'));

//include services
include_once __DIR__ . '/lib/Services.php';

//routing
include_once __DIR__ . '/lib/Routing.php';

//Bootstrapping
Service::setInstance(new Service($app['config']['google_api']));

$app->run();
