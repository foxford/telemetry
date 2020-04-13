# Telemetry

[![Build Status][travis-img]][travis]

The microservice is collecting telemetry data.


## Getting started

To send a telemetry message
```bash
mosquitto_pub -V 5 \
    -i 'test-pub.john-doe.usr.example.net' \
    -t 'agents/test-pub.john-doe.usr.example.net/api/v1/out/telemetry.svc.example.org' \
    -D connect user-property 'connection_version' 'v2' \
    -D connect user-property 'connection_mode' 'default' \
    -D publish user-property 'label' 'ping' \
    -D publish user-property 'local_timestamp' "$(date +%s000)" \
    -m '{"metric": "apps.ulms-p2p.fps", "value": 60.0, "tags": {"summary": "min"}}'

mosquitto_pub -V 5 \
    -i 'test-pub.john-doe.usr.example.net' \
    -t 'agents/test-pub.john-doe.usr.example.net/api/v1/out/telemetry.svc.example.org' \
    -D connect user-property 'connection_version' 'v2' \
    -D connect user-property 'connection_mode' 'default' \
    -D publish user-property 'label' 'ping' \
    -D publish user-property 'local_timestamp' "$(date +%s000)" \
    -m '[{"metric": "apps.ulms-p2p.fps", "value": 60.0, "tags": {"summary": "min"}}, {"metric": "apps.ulms-p2p.fps", "value": 60.0, "tags": {"summary": "max"}}]'
```

To send a general purpose message
```bash
mosquitto_pub -V 5 \
    -i 'test-pub.john-doe.usr.example.net' \
    -t 'agents/test-pub.john-doe.usr.example.net/api/v1/out/app.svc.example.org' \
    -D connect user-property 'connection_version' 'v2' \
    -D connect user-property 'connection_mode' 'default' \
    -D publish user-property 'label' 'ping' \
    -D publish user-property 'local_timestamp' "$(date +%s000)" \
    -m '{"id": "123e4567-e89b-12d3-a456-426655440000", "object": {"foo": "bar"}, "list": [1, 2, 3], "boolean": true, "float": 0.12, "int": 12, "null": null}'
```



## License

The source code is provided under the terms of [the MIT license][license].

[license]:http://www.opensource.org/licenses/MIT
[travis]:https://travis-ci.com/netology-group/telemetry?branch=master
[travis-img]:https://travis-ci.com/netology-group/telemetry.png?branch=master
