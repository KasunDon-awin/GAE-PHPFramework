<?php

namespace Controllers;

use Entity\Feed;

class DefaultController extends \Lib\ControllerAbstract {

    public function indexAction() {
        $feed_url = 'http://www.sciam.com/xml/sciam.xml';
        $feed_model = new Feed();
        $feed_model->setSubscriber_url($feed_url);
        $feed_model->setAge('11');
        $feed_model->setName('hello world!');
       // $feed_model->setKeyName($feed_model->getSubscriberUrl());
        $feed_model->put();
        //$data = $feed_model->fetchBy('12', 'AGE');
        //$results = array_pop($data);
        return $this->app['twig']->render('hello.twig', ['word' => null]);
    }

}
