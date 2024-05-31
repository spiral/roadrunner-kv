<a href="https://roadrunner.dev" target="_blank">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/roadrunner-server/.github/assets/8040338/e6bde856-4ec6-4a52-bd5b-bfe78736c1ff">
    <img align="center" src="https://github.com/roadrunner-server/.github/assets/8040338/040fb694-1dd3-4865-9d29-8e0748c2c8b8">
  </picture>
</a>

# RoadRunner KV Plugin Bridge

[![PHP Version Require](https://poser.pugx.org/spiral/roadrunner-kv/require/php)](https://packagist.org/packages/spiral/roadrunner-kv)
[![Latest Stable Version](https://poser.pugx.org/spiral/roadrunner-kv/v/stable)](https://packagist.org/packages/spiral/roadrunner-kv)
[![phpunit](https://github.com/spiral/roadrunner-kv/actions/workflows/phpunit.yml/badge.svg)](https://github.com/spiral/roadrunner-kv/actions)
[![psalm](https://github.com/spiral/roadrunner-kv/actions/workflows/psalm.yml/badge.svg)](https://github.com/spiral/roadrunner-kv/actions)
[![Total Downloads](https://poser.pugx.org/spiral/roadrunner-kv/downloads)](https://packagist.org/packages/spiral/roadrunner-kv)

<b>[Documentation](https://docs.roadrunner.dev/key-value/overview-kv)</b> | [Framework Bundle](https://github.com/spiral/framework)

This repository contains the codebase PSR-16 PHP cache bridge using kv RoadRunner plugin.

## Installation

To install application server and KV codebase

```bash
composer require spiral/roadrunner-kv
```

You can use the convenient installer to download the latest available compatible
version of RoadRunner server:

```bash
composer require spiral/roadrunner-cli --dev
vendor/bin/rr get
```

## Usage

First you need to add at least one kv plugin to your roadrunner configuration. 
For example, such a configuration would be quite feasible to run:

```yaml
rpc:
  listen: tcp://127.0.0.1:6001

kv:
  test:
    driver: memory
    config:
        interval: 10
```

> **Note**
> Read more about all available drivers on the 
> [documentation](https://docs.roadrunner.dev) page.

After starting the server with this configuration, one driver named "`test`" 
will be available to you.

The following code will allow writing and reading an arbitrary value from the 
RoadRunner server.

```php
<?php

use Spiral\Goridge\RPC\RPC;
use Spiral\RoadRunner\KeyValue\Factory;

require __DIR__ . '/vendor/autoload.php';

$factory = new Factory(RPC::create('tcp://127.0.0.1:6001'));

$cache = $factory->select('test');

// After that you can write and read arbitrary values:

$cache->set('key', 'value');

echo $cache->get('key'); // string(5) "value"
```

<a href="https://spiral.dev/">
<img src="https://user-images.githubusercontent.com/773481/220979012-e67b74b5-3db1-41b7-bdb0-8a042587dedc.jpg" alt="try Spiral Framework" />
</a>

## License:

MIT License (MIT). Please see [`LICENSE`](./LICENSE) for more information. Maintained by [Spiral Scout](https://spiralscout.com).
