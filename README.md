Simple example demonstrating a problem with Enhanced Fan-Out KCL Consumers against Localstack

Based on https://docs.aws.amazon.com/streams/latest/dev/building-enhanced-consumers-kcl-java.html but tweaked to work with Localstack.

Duplication steps:

1. Start localstack:
```bash
docker-compose up --build -d
```

2. Create a Kinesis stream:
```bash
aws kinesis create-stream --stream-name efo-test --profile localstack
```

3. Run the application and wait for the errors to happen. The time it takes for the error to happen is equal to the `apiCallTimeout` setting on the Kinesis client
