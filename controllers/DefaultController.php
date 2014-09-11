<?php

namespace Controllers;

class DefaultController extends \Lib\ControllerAbstract {

    public function indexAction() {
        return $this->app['twig']->render('hello.twig', ['word' => 'world!']);
    }

}
