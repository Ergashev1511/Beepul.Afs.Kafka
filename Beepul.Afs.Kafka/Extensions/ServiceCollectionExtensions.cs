using Beepul.Afs.Kafka.Abstractions;
using Beepul.Afs.Kafka.Consuming;
using Beepul.Afs.Kafka.Options;
using Beepul.Afs.Kafka.Producing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Beepul.Afs.Kafka.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddKafkaPublisher<TEvent>(
           this IServiceCollection services,
           IConfiguration config,
           string sectionName = "Kafka:Publisher")
           where TEvent : IKafkaEvent
        {
            services.Configure<KafkaPublisherOptions>(config.GetSection(sectionName));
            services.AddSingleton<IEventPublisher<TEvent>, KafkaPublisher<TEvent>>();
            return services;
        }
        public static IServiceCollection AddKafkaBatchConsumer<TEvent, THandler>(
            this IServiceCollection services,
            IConfiguration config,
            string sectionName = "Kafka:Consumer")
            where TEvent : IKafkaEvent
            where THandler : class, IBatchEventHandler<TEvent>
        {
            services.Configure<KafkaConsumerOptions>(config.GetSection(sectionName));
            services.AddSingleton<IBatchEventHandler<TEvent>, THandler>();
            services.AddHostedService<KafkaBatchConsumerService<TEvent>>();
            return services;

        }
    }
}
