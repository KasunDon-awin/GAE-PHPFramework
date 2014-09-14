<?php

namespace Controllers;

use Entity\Feed;

class DefaultController extends \Lib\ControllerAbstract {

    public function indexAction() {
        $feed_url = 'http://www.sciam.com/xml/sciam.xml';
        $feed_model = new Feed($feed_url);

// save the instance to the datastore
        $feed_model->put();

// now, try fetching the saved model from the datastore

        $kname = sha1($feed_url);
// fetch the feed with that key, as part of the transaction
        $feed_model_fetched = Feed::fetchByName($kname);

        $feed_model_fetched = $feed_model_fetched[0];

        return $this->app['twig']->render('hello.twig', ['word' => $feed_model_fetched->getSubscriberUrl()]);
    }

}
