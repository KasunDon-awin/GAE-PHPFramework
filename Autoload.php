<?php

require_once 'vendor/autoload.php';

function autoload($classId) {
    $classIdParts = explode("\\", $classId);
    $classIdLength = count($classIdParts);
    $className = strtolower($classIdParts[$classIdLength - 1]);
    $namespace = strtolower($classIdParts[0]);

    for ($i = 1; $i < $classIdLength - 1; $i++) {
        $namespace .= '/' . $classIdParts[$i];
    }

    if (file_exists(dirname(__FILE__))
            . '/' . $namespace
            . '/' . $className . '.php') {
        include $namespace . '/' . $className . '.php';
    }
}

spl_autoload_register('autoload');
