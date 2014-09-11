<?php

$app['default.controller'] = $app->share(function() use ($app) {
    return new Controllers\DefaultController($app);
});
