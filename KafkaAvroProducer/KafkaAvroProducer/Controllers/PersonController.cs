using Avro;
using Avro.Generic;
using com.example;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using KafkaAvroProducer.Models;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace KafkaAvroProducer.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PersonController(
            ILoggerFactory loggerFactory,
            IConfiguration configuration,
            ProducerConfig producerConfig) : ControllerBase
    {
        private readonly ILogger<PersonController> _logger = LoggerFactoryExtensions.CreateLogger<PersonController>(loggerFactory);

        // POST api/<PersonController>/specific
        [HttpPost("/specific")]
        public async Task<IActionResult> PostSpecificAsync([FromBody] OrdinaryPerson ordinaryPerson)
        {
            string resultMessage;

            try
            {
                var topicName = configuration.GetSection("ConfluentCloud:Topic").Value;
                var schemaRegistryApiKey = configuration.GetSection("ConfluentCloud:SchemaRegistryApiKey").Value;
                var schemaRegistryApiSecret = configuration.GetSection("ConfluentCloud:SchemaRegistryApiSecret").Value;

                var schemaRegistryConfig = new SchemaRegistryConfig
                {
                    // Note: you can specify more than one schema registry url using the
                    // schema.registry.url property for redundancy (comma separated list).
                    Url = configuration.GetSection("ConfluentCloud:SchemaRegistryUrl").Value,
                    BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
                    BasicAuthUserInfo = $"{schemaRegistryApiKey}:{schemaRegistryApiSecret}"
                };

                var avroSerializerConfig = new AvroSerializerConfig
                {
                    SubjectNameStrategy = SubjectNameStrategy.Record // do not bind the schema to the topic
                };

                // Note: Create an ExtraOrdinaryPerson from OrdinaryPerson
                var person = new ExtraOrdinaryPerson
                {
                    Id = ordinaryPerson.Id,
                    Name = ordinaryPerson.Name,
                    Age = ordinaryPerson.Age,
                    Email = ordinaryPerson.Email,
                    Address = ordinaryPerson.Address
                };

                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                using (var producer =
                                new ProducerBuilder<string, ExtraOrdinaryPerson>(producerConfig)
                                    .SetValueSerializer(new AvroSerializer<ExtraOrdinaryPerson>(schemaRegistry, avroSerializerConfig))
                                    .Build())
                {
                    var result = await producer
                        .ProduceAsync(topicName, new Message<string, ExtraOrdinaryPerson> {
                            Key = person.Id.ToString(),
                            Value = person
                        });

                    _logger.LogInformation($"Status: {result.Status}");
                    _logger.LogInformation($"TopicPartition: {result.Partition}");
                    _logger.LogInformation($"TopicPartitionOffset: {result.TopicPartitionOffset}");
                }

                return Ok();
            }
            catch (ProduceException<string, ExtraOrdinaryPerson> ex)
            {
                resultMessage =
                    $"Failed to deliver message: {ex.Message} [{ex.Error.Code}] for message (value: '{ex.DeliveryResult.Value}')";

                _logger.LogError(ex, resultMessage);

                return StatusCode(500, resultMessage);
            }
        }

        // POST api/<PersonController>/specific
        [HttpPost("/generic")]
        public async Task<IActionResult> PostGenericAsync([FromBody] OrdinaryPerson ordinaryPerson)
        {
            string resultMessage;

            try
            {
                var record = new GenericRecord((RecordSchema)ExtraOrdinaryPerson._SCHEMA);
                record.Add("Id", ordinaryPerson.Id);
                record.Add("Name", ordinaryPerson.Name);
                record.Add("Age", ordinaryPerson.Age);
                record.Add("Email", ordinaryPerson.Email);
                record.Add("Address", ordinaryPerson.Address);

                var topicName = configuration.GetSection("ConfluentCloud:Topic").Value;
                var schemaRegistryApiKey = configuration.GetSection("ConfluentCloud:SchemaRegistryApiKey").Value;
                var schemaRegistryApiSecret = configuration.GetSection("ConfluentCloud:SchemaRegistryApiSecret").Value;

                var schemaRegistryConfig = new SchemaRegistryConfig
                {
                    // Note: you can specify more than one schema registry url using the
                    // schema.registry.url property for redundancy (comma separated list). 
                    Url = configuration.GetSection("ConfluentCloud:SchemaRegistryUrl").Value,
                    BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
                    BasicAuthUserInfo = $"{schemaRegistryApiKey}:{schemaRegistryApiSecret}"
                };

                var avroSerializerConfig = new AvroSerializerConfig
                {
                    SubjectNameStrategy = SubjectNameStrategy.Record // do not bind the schema to the topic
                };

                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                using (var producer =
                                new ProducerBuilder<string, GenericRecord>(producerConfig)
                                    .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, avroSerializerConfig))
                                    .Build())
                {
                    var result = await producer
                        .ProduceAsync(topicName, new Message<string, GenericRecord>
                        {
                            Key = ordinaryPerson.Id.ToString(),
                            Value = record
                        });

                    _logger.LogInformation($"Status: {result.Status}");
                    _logger.LogInformation($"TopicPartition: {result.Partition}");
                    _logger.LogInformation($"TopicPartitionOffset: {result.TopicPartitionOffset}");
                }

                return Ok();

            }
            catch (ProduceException<string, GenericRecord> ex)
            {
                resultMessage =
                    $"Failed to deliver message: {ex.Message} [{ex.Error.Code}] for message (value: '{ex.DeliveryResult.Value}')";

                _logger.LogError(ex, resultMessage);

                return StatusCode(500, resultMessage);
            }
        }
    }
}
