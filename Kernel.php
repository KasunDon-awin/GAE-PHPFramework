<?php

require_once __DIR__ . '/Autoload.php';

$app = new Silex\Application();

//registering controller service provider
$app->register(new Silex\Provider\ServiceControllerServiceProvider());

//registering views
$app->register(new Silex\Provider\TwigServiceProvider(), array(
    'twig.path' => __DIR__.'/views',
));

//include services
include_once __DIR__ . '/lib/Services.php';

//routing
include_once __DIR__ . '/lib/Routing.php';

$app->run();
