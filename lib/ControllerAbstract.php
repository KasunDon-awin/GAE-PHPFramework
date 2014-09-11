<?php

namespace Lib;

use Silex\Application as Application;

abstract class ControllerAbstract {

    protected $app;

    public function __construct(Application $app) {
        $this->app = $app;
    }

}
