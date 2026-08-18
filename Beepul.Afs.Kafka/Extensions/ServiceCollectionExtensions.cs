using Beepul.Afs.Kafka.Abstractions;
using Beepul.Afs.Kafka.Consuming;
using Beepul.Afs.Kafka.Options;
using Beepul.Afs.Kafka.Producing;
using Beepul.Afs.Kafka.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Beepul.Afs.Kafka.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddKafkaPublisher(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = KafkaPublisherOptions.DefaultSectionName)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(sectionName);

        services.AddOptions<KafkaPublisherOptions>()
            .Bind(configuration.GetSection(sectionName))
            .ValidateOnStart();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<KafkaPublisherOptions>, KafkaPublisherOptionsValidator>());
        services.TryAddSingleton<IKafkaEventSerializer, SystemTextJsonKafkaEventSerializer>();
        services.AddSingleton<IEventPublisher, KafkaPublisher>();
        return services;
    }

    public static IServiceCollection AddKafkaBatchConsumer<TPayload, THandler>(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = KafkaConsumerOptions.DefaultSectionName)
        where THandler : class, IBatchEventHandler<TPayload>
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(sectionName);

        services.AddOptions<KafkaConsumerOptions>()
            .Bind(configuration.GetSection(sectionName))
            .ValidateOnStart();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<KafkaConsumerOptions>, KafkaConsumerOptionsValidator>());
        services.TryAddSingleton<IKafkaEventSerializer, SystemTextJsonKafkaEventSerializer>();
        services.AddSingleton<IBatchEventHandler<TPayload>, THandler>();
        services.AddHostedService<KafkaBatchConsumerService<TPayload>>();
        return services;
    }

    public static IServiceCollection AddKafkaEventSerializer<TSerializer>(this IServiceCollection services)
        where TSerializer : class, IKafkaEventSerializer
    {
        ArgumentNullException.ThrowIfNull(services);
        services.Replace(ServiceDescriptor.Singleton<IKafkaEventSerializer, TSerializer>());
        return services;
    }
}
