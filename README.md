# Telemetry

[![Build Status][travis-img]][travis]

The microservice is collecting telemetry data.


## Getting started

To send a message
```bash
mosquitto_pub -V 5 \
    -i 'test-pub.john-doe.usr.example.net' \
    -t 'agents/test-pub.john-doe.usr.example.net/api/v1/out/telemetry.svc.example.org' \
    -D connect user-property 'connection_version' 'v2' \
    -D connect user-property 'connection_mode' 'default' \
    -D publish user-property 'label' 'ping' \
    -D publish user-property 'local_timestamp' "$(date +%s000)" \
    -m '{"avg":60.0, "max":60.0, "min":60.0, "object":["apps","ulms-p2p","fps"]}'
```



## License

The source code is provided under the terms of [the MIT license][license].

[license]:http://www.opensource.org/licenses/MIT
[travis]:https://travis-ci.com/netology-group/telemetry?branch=master
[travis-img]:https://travis-ci.com/netology-group/telemetry.png?branch=master
