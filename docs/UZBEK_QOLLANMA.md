# Beepul.Afs.Kafka — batafsil o‘zbekcha qo‘llanma

## 1. Package vazifasi

`Beepul.Afs.Kafka` — .NET 8 ilovalari uchun Kafka eventlarini yuborish va ularni batch ko‘rinishida qabul qilish kutubxonasi.

Package quyidagi vazifalarni bajaradi:

- typed payloadni umumiy event envelope ichida JSON ko‘rinishiga keltiradi;
- optional Kafka key bilan event yuboradi;
- producer’ning native batching, compression va idempotence imkoniyatlaridan foydalanadi;
- Kafka’dan eventlarni count, byte hajmi yoki vaqt limiti bo‘yicha batchlab oladi;
- batch handler muvaffaqiyatli tugagandan keyingina offsetlarni commit qiladi;
- vaqtinchalik handler xatolarida batchni retry qiladi va offsetni commit qilmaydi;
- buzilgan yoki permanent xatoli eventlarni ishonchli DLQ topic’ga yuboradi;
- DLQ delivery tasdiqlanmasa original offsetni commit qilmaydi.

Package ClickHouse’ga bevosita yozmaydi. ClickHouse insert consumer ilovasidagi `IBatchEventHandler<TPayload>` implementatsiyasida bajariladi.

## 2. Yetkazib berish kafolati

Package `at-least-once` processing modelida ishlaydi:

```text
Event handlerga kamida bir marta yetkaziladi.
Event yo‘qolmasligi ustuvor.
Ayrim nosozliklarda event takroran kelishi mumkin.
```

Kafka va ClickHouse umumiy transaction ishlatmaydi. Quyidagi holat yuz berishi mumkin:

```text
1. ClickHouse insert muvaffaqiyatli bo‘ldi
2. Kafka offset commit vaqtincha xato berdi
3. Consumer qayta ishga tushdi
4. Kafka o‘sha eventni qayta berdi
5. ClickHouse bir xil EventId’ni ikkinchi marta ko‘rdi
```

Shuning uchun ClickHouse handler `EventId` bo‘yicha idempotent bo‘lishi shart.

## 3. Event envelope

Package yuboradigan umumiy model:

```csharp
public sealed record KafkaEvent<TPayload>
{
    public Guid EventId { get; }
    public string EventType { get; }
    public DateTimeOffset OccurredAt { get; }
    public TPayload Payload { get; }
}
```

### EventId

Har bir biznes hodisaning unique identifikatori.

```text
transfer.created    → EventId=A
transfer.authorized → EventId=B
transfer.paid       → EventId=C
```

Retry vaqtida `EventId` o‘zgarmasligi kerak. Production’da uni business transaction yoki outbox record yaratilganda bir marta yaratish tavsiya qilinadi.

### EventType

Hodisa turining stable nomi:

```text
transfer.created
transfer.authorized
transfer.paid
session.started
session.closed
```

`.NET` class nomini event contract sifatida ishlatmaslik ma’qul, chunki refactoring Kafka contract’ini tasodifan o‘zgartirishi mumkin.

### OccurredAt

Hodisa biznes tizimda sodir bo‘lgan vaqt. Kafka’ga yuborilgan yoki ClickHouse’ga yozilgan vaqt emas.

```csharp
occurredAt: DateTimeOffset.UtcNow
```

UTC ishlatish tavsiya qilinadi.

### Payload

Domain ma’lumoti. U har qanday typed model bo‘lishi mumkin:

```text
KafkaEvent<Transfer>
KafkaEvent<Session>
KafkaEvent<BusinessEvent>
```

Model evolyutsiyasi schemasiz ishlashi uchun yangi maydonlar nullable yoki default qiymatli qo‘shilishi kerak. Property rename, type change yoki yangi required property eski Kafka JSON’larini buzishi mumkin.

## 4. Kafka key

Kafka key envelope’dan alohida va optional:

```csharp
await publisher.PublishAsync(
    topic,
    envelope,
    key: $"transfer:{transfer.TransferId}",
    cancellationToken);
```

Key berilmasa:

```csharp
key: null
```

Kafka key partition tanlash uchun ishlatiladi:

```text
partition = hash(key) % partitionCount
```

Bir transferning `created`, `authorized`, `paid` eventlari uchun bir xil key berilsa, ular bir partitionga tushadi va tartibi saqlanadi:

```text
key=transfer:123 → created → authorized → paid
```

Session uchun:

```text
key=session:{SessionId}
```

Tartib yoki entity affinity kerak bo‘lmasa `null` key ishlatish mumkin.

## 5. To‘liq ishlash oqimi

```text
HTTP API / Business service
            │
            ▼
    KafkaEvent<TPayload>
            │
            ▼
       IEventPublisher
            │
            ├── JSON serialization
            ├── optional Kafka key
            ├── event-id header
            └── event-type header
            │
            ▼
         Kafka topic
            │
            ▼
  key hash orqali partition
            │
            ▼
      Kafka broker/cluster
            │
            ▼
       Consumer group
            │
            ▼
       Batch collector
   count / bytes / timeout
            │
            ▼
 IBatchEventHandler<TPayload>
            │
       ┌────┴─────┐
       │          │
    Success     Exception
       │          │
       ▼          ▼
 Offset commit  Commit yo‘q
                  │
                  ▼
                Retry
```

## 6. Publisher’dan foydalanish

DI registratsiyasi:

```csharp
builder.Services.AddKafkaPublisher(builder.Configuration);
```

Publisher singleton sifatida ishlaydi. Har event uchun yangi native producer yaratmaydi.

### Bitta event yuborish

```csharp
var envelope = new KafkaEvent<Transfer>(
    eventId: outboxEventId,
    eventType: "transfer.created",
    occurredAt: DateTimeOffset.UtcNow,
    payload: transfer);

await publisher.PublishAsync(
    topic: "beepul.transfer.events",
    @event: envelope,
    key: $"transfer:{transfer.TransferId}",
    cancellationToken);
```

### Ko‘p event yuborish

```csharp
var requests = events.Select(item =>
    new KafkaPublishRequest<Transfer>(
        topic: "beepul.transfer.events",
        @event: item,
        key: $"transfer:{item.Payload.TransferId}"))
    .ToArray();

await publisher.PublishBatchAsync(requests, cancellationToken);
```

`PublishBatchAsync` requestlarni bir vaqtda native producer queue’siga beradi. Kafka ularni topic, partition va byte hajmi bo‘yicha ichki network batchlarga ajratadi.

## 7. Consumer’dan foydalanish

Handler:

```csharp
public sealed class TransferBatchHandler
    : KafkaEventHandler<Transfer>
{
    public override async Task HandleAsync(
        IReadOnlyList<KafkaEvent<Transfer>> batch,
        CancellationToken cancellationToken)
    {
        await InsertIntoClickHouseIdempotently(
            batch,
            cancellationToken);
    }
}
```

DI registratsiyasi:

```csharp
builder.Services.AddKafkaBatchConsumer<Transfer, TransferBatchHandler>(
    builder.Configuration);
```

Handler muvaffaqiyatli qaytsa batch offsetlari commit qilinadi. Oddiy exception tashlansa transient failure hisoblanadi va retry qilinadi.

Qayta urinish foyda bermaydigan buzilgan ma’lumot uchun:

```csharp
throw new PermanentException("Unsupported transfer data");
```

Bu batchni DLQ topic’ga yuboradi. DLQ tasdiqlangandan keyingina original offset commit qilinadi.

## 8. Batch qanday yig‘iladi?

Batch quyidagi limitlardan birinchisiga yetganda yopiladi:

```text
MaxBatchSize
YOKI
MaxBatchBytes
YOKI
BatchTimeout
```

Misol:

```text
MaxBatchSize  = 50 000
MaxBatchBytes = 64 MB
BatchTimeout  = 3 sekund
```

30 000 event/sekund oqimda count limit bo‘lmasa 3 sekundda taxminan 90 000 event keladi. `MaxBatchSize=50 000` bo‘lgani uchun batch timeoutni kutmasdan taxminan 1.67 sekundda yopiladi.

`MaxBatchBytes` tekshiruvi record olingandan keyin yangilanadi. Shu sabab batch byte limiti bitta record hajmicha oshib ketishi mumkin; eventning o‘zi bo‘linmaydi.

## 9. Offset boshqaruvi

Package’da:

```text
EnableAutoCommit=false
EnableAutoOffsetStore=false
```

Bu qiymatlar delivery kafolatining invariantlari va foydalanuvchi konfiguratsiyasiga chiqarilmagan.

Handler success’dan keyin batchdagi har partition uchun eng katta offset topiladi:

```text
Partition 0: max offset 150 → commit 151
Partition 1: max offset 220 → commit 221
Partition 2: max offset 98  → commit 99
```

Committed offset Kafka uchun keyingi o‘qiladigan record manzilidir.

Commit xato bersa package yangi event consume qilmaydi va commitni `CommitRetryDelay` bilan qayta urinadi.

## 10. Uzoq handler va partition pause

Batch ClickHouse’da ishlanayotgan vaqtda barcha joriy assigned partitionlar pause qilinadi. Bu consumer polling vaqtida keyingi eventlarni tasodifan olib, tashlab yubormaslik uchun kerak.

Handler kutilayotgan yoki retry delay davom etayotgan paytda package Kafka’ni `ProcessingPollInterval` bo‘yicha poll qilishda davom etadi. Bu consumer group membership va `max.poll.interval.ms` talablarini ushlab turishga yordam beradi.

Batch tugagach hali consumerga assigned bo‘lgan partitionlar resume qilinadi.

## 11. Transient va permanent failure

### Transient failure

Misollar:

- ClickHouse vaqtincha ishlamayapti;
- network timeout;
- connection pool vaqtincha band;
- ClickHouse overload.

Handler oddiy exception tashlaydi:

```text
Handler exception
      ↓
Offset commit qilinmaydi
      ↓
Exponential backoff
      ↓
O‘sha batch qayta ishlanadi
```

### Permanent failure

Misollar:

- payload biznes qoidasi bo‘yicha mutlaqo yaroqsiz;
- retry bilan tuzalmaydigan data format;
- qo‘llab-quvvatlanmaydigan event turi.

Handler `PermanentException` tashlaydi:

```text
PermanentException
      ↓
DLQ'ga ProduceAsync
      ↓
Kafka Persisted tasdig‘i
      ↓
Original offset commit
```

DLQ sozlanmagan yoki DLQ write xato bo‘lsa original offset commit qilinmaydi.

### Deserialization failure

JSON `KafkaEvent<TPayload>`ga deserialize bo‘lmasa handler chaqirilmaydi. Record DLQ’ga yuboriladi. DLQ tasdiqlanmasa commit bajarilmaydi.

## 12. Publisher options

Konfiguratsiya bo‘limi: `Kafka:Publisher`.

### BootstrapServers

```json
"BootstrapServers": "localhost:9092"
```

Kafka clusterga dastlabki ulanish manzillari. Production’da bir nechta broker berish mumkin:

```json
"BootstrapServers": "kafka-1:9092,kafka-2:9092,kafka-3:9092"
```

Bu faqat bootstrap ro‘yxati; producer cluster metadata orqali qolgan brokerlarni topadi.

### ClientId

```json
"ClientId": "afs-publisher"
```

Kafka broker loglari va metrics’da producer’ni aniqlash uchun ishlatiladigan nom. Har service uchun tushunarli va stable nom berish kerak.

### LingerMs

```json
"LingerMs": 10
```

Producer ko‘proq recordni bitta network batchga yig‘ish uchun kutishi mumkin bo‘lgan vaqt.

- kichik qiymat: latency pastroq, request soni ko‘proq;
- katta qiymat: throughput/compression yaxshiroq, latency yuqoriroq.

High-throughput analytics uchun odatda 5–20 ms boshlang‘ich qiymat sifatida sinov qilinadi.

### BatchSizeBytes

```json
"BatchSizeBytes": 524288
```

Producer’ning bir partition uchun ichki batch byte hajmi. `524288` = 512 KiB.

Bu application `PublishBatchAsync` request soni emas. Native producer network batching parametri.

### Acks

```json
"Acks": "All"
```

Broker tasdig‘i talabi:

- `None`: tasdiq kutilmaydi, data yo‘qotish xavfi eng yuqori;
- `Leader`: faqat partition leader tasdiqlaydi;
- `All`: barcha ISR replica tasdiqlaydi.

Transfer eventlari uchun `All` tavsiya qilinadi.

### CompressionType

```json
"CompressionType": "Snappy"
```

Producer batch compression algoritmi. Qo‘llab-quvvatlanadigan asosiy qiymatlar:

```text
None
Gzip
Snappy
Lz4
Zstd
```

`Snappy` tezlik va compression o‘rtasida yaxshi balans beradi. Haqiqiy payload bilan `Lz4` va `Zstd`ni ham benchmark qilish mumkin.

### EnableIdempotence

```json
"EnableIdempotence": true
```

Producer retrylari sabab bir producer session ichida duplicate record yozilish xavfini kamaytiradi. `true` bo‘lsa `Acks=All` talab qilinadi va options validator buni tekshiradi.

Bu ClickHouse idempotency o‘rnini bosmaydi.

### MessageSendMaxRetries

```json
"MessageSendMaxRetries": 5
```

Native producer delivery muvaffaqiyatsiz bo‘lganda Kafka clientning maksimal qayta urinishlari.

### MessageTimeoutMs

```json
"MessageTimeoutMs": 30000
```

Event delivery uchun umumiy maksimal vaqt. Shu muddat ichida delivery bo‘lmasa publisher exception qaytaradi.

### FlushTimeout

```json
"FlushTimeout": "00:00:10"
```

Application yopilayotganda producer queue’dagi eventlarni Kafka’ga yetkazish uchun kutiladigan maksimal vaqt.

## 13. Consumer options

Konfiguratsiya bo‘limi: `Kafka:Consumer`.

### BootstrapServers

Consumer ulanishi uchun Kafka broker manzillari. Publisher bilan bir xil cluster bo‘lishi shart emas, lekin odatda bir xil bo‘ladi.

### GroupId

```json
"GroupId": "afs-clickhouse-writer-v1"
```

Consumer group identifikatori. Bir xil `GroupId`dagi instance’lar topic partitionlarini o‘zaro bo‘lib oladi.

```text
6 partition + 3 consumer = har consumerga taxminan 2 partition
```

Yangi `GroupId` eski committed offsetlarni ishlatmaydi va `AutoOffsetReset` siyosatiga o‘tadi.

### Topic

```json
"Topic": "beepul.transfer.events"
```

Consumer subscribe qiladigan asosiy topic.

### DeadLetterTopic

```json
"DeadLetterTopic": "beepul.transfer.events.dlq"
```

Malformed yoki permanent eventlar yuboriladigan topic. `null` yoki bo‘sh bo‘lsa permanent/deserialization failure vaqtida package xavfsizlik uchun offsetni commit qilmaydi.

### MaxBatchSize

```json
"MaxBatchSize": 50000
```

Bitta application batchdagi maksimal event soni.

Hisoblashda:

```text
Target batch time × event/second
```

30 000 event/s va taxminiy 2 sekundlik batch uchun:

```text
30 000 × 2 = 60 000
```

ClickHouse insert latency va process memory bilan benchmark qilish kerak.

### InitialBatchCapacity

```json
"InitialBatchCapacity": 50000
```

Consumer batch `List`i uchun boshida reserve qilinadigan capacity. Bu batch limiti emas.

Haqiqiy initial capacity:

```text
min(MaxBatchSize, InitialBatchCapacity)
```

Katta qiymat list resize/copy sonini kamaytiradi, lekin batch hali to‘lmasidan oldin ko‘proq memory reserve qiladi.

### MaxBatchBytes

```json
"MaxBatchBytes": 67108864
```

Bitta raw Kafka batchining maksimal byte hajmi. `67108864` = 64 MiB.

Count kichik bo‘lsa ham payloadlar katta bo‘lishi mumkin. Bu parametr process memory’ni himoya qiladi.

Taxminiy batch memory bundan yuqori bo‘ladi, chunki raw bytes, deserialized objectlar, listlar va handler bufferlari bir vaqtda mavjud bo‘lishi mumkin.

### BatchTimeout

```json
"BatchTimeout": "00:00:03"
```

Batch yig‘ish oynasining maksimal davomiyligi. Trafik past bo‘lsa ham eventlar cheksiz kutilmaydi.

### ConsumePollInterval

```json
"ConsumePollInterval": "00:00:00.200"
```

Batch yig‘ishda bitta `Consume` chaqirig‘i broker event bermasa qancha kutishi mumkinligini belgilaydi.

- kichik qiymat: cancellation va timeoutga tezroq javob;
- juda kichik qiymat: ko‘proq bo‘sh poll;
- katta qiymat: shutdown/timeout reaksiyasi sekinroq.

### ProcessingPollInterval

```json
"ProcessingPollInterval": "00:00:01"
```

Handler yoki retry delay kutilayotgan va partitionlar pause qilingan paytda consumer Kafka’ni qanchada bir poll qilishini belgilaydi.

### AutoOffsetReset

```json
"AutoOffsetReset": "Earliest"
```

Consumer group uchun committed offset topilmaganda qayerdan boshlash:

- `Earliest`: topicdagi mavjud eng eski eventdan;
- `Latest`: faqat yangi keladigan eventlardan;
- `Error`: offset yo‘q bo‘lsa xato.

Eventlarni yo‘qotmaslik uchun yangi ClickHouse consumer group’da `Earliest` tavsiya qilinadi.

### PartitionAssignmentStrategy

```json
"PartitionAssignmentStrategy": "CooperativeSticky"
```

Consumer group partitionlarni instance’lar orasida qanday taqsimlashini belgilaydi.

- `Range`: topic partitionlarini range bo‘yicha beradi;
- `RoundRobin`: partitionlarni aylana bo‘yicha taqsimlaydi;
- `CooperativeSticky`: rebalance paytida imkon qadar mavjud assignmentni saqlaydi va bosqichma-bosqich ko‘chiradi.

Uzoq ishlaydigan batch consumerlar uchun `CooperativeSticky` tavsiya qilinadi.

### MaxRetryAttempts

```json
"MaxRetryAttempts": 0
```

- `0`: cheksiz retry;
- musbat `N`: `N` marta muvaffaqiyatsiz handler execution’dan keyin consumer exception bilan to‘xtaydi.

ClickHouse vaqtinchalik uzilishida event Kafka’da qolishi kerak bo‘lsa `0` tavsiya qilinadi.

### InitialRetryDelay

```json
"InitialRetryDelay": "00:00:00.500"
```

Birinchi transient failure’dan keyingi kutish vaqti.

### MaxRetryDelay

```json
"MaxRetryDelay": "00:00:30"
```

Exponential backoff uchun maksimal kutish vaqti.

### RetryBackoffMultiplier

```json
"RetryBackoffMultiplier": 2
```

Har xatodan keyin delay ko‘paytirgichi:

```text
500 ms → 1 s → 2 s → 4 s → 8 s → ... → 30 s
```

`1` berilsa delay o‘zgarmaydi.

### MaxPollInterval

```json
"MaxPollInterval": "00:15:00"
```

Kafka consumer `poll` chaqiriqlari orasidagi ruxsat etilgan maksimal vaqt. Oshib ketsa broker consumer’ni group’dan chiqarishi va rebalance boshlashi mumkin.

Package handler vaqtida pollingni davom ettiradi, ammo qiymat baribir eng yomon handler/retry/shutdown holatlari bilan mos bo‘lishi kerak.

### CommitRetryDelay

```json
"CommitRetryDelay": "00:00:01"
```

Handler success’dan keyin offset commit vaqtincha xato bersa, commit qayta urinishlari orasidagi delay.

### DeadLetterAcks

```json
"DeadLetterAcks": "All"
```

DLQ producer uchun broker tasdig‘i. Event yo‘qolmasligi uchun `All` tavsiya qilinadi.

### DeadLetterEnableIdempotence

```json
"DeadLetterEnableIdempotence": true
```

DLQ producer session ichidagi retry duplicate’larini kamaytiradi. `true` bo‘lsa `DeadLetterAcks=All` validator orqali talab qilinadi.

### DeadLetterMessageTimeoutMs

```json
"DeadLetterMessageTimeoutMs": 30000
```

DLQ event delivery uchun maksimal vaqt. Timeout yoki delivery failure bo‘lsa original offset commit qilinmaydi.

### DeadLetterFlushTimeout

```json
"DeadLetterFlushTimeout": "00:00:10"
```

Application yopilayotganda DLQ producer queue’sini flush qilish uchun maksimal kutish.

## 14. To‘liq konfiguratsiya namunasi

```json
{
  "Kafka": {
    "Publisher": {
      "BootstrapServers": "localhost:9092",
      "ClientId": "afs-publisher",
      "LingerMs": 10,
      "BatchSizeBytes": 524288,
      "Acks": "All",
      "CompressionType": "Snappy",
      "EnableIdempotence": true,
      "MessageSendMaxRetries": 5,
      "MessageTimeoutMs": 30000,
      "FlushTimeout": "00:00:10"
    },
    "Consumer": {
      "BootstrapServers": "localhost:9092",
      "GroupId": "afs-clickhouse-writer-v1",
      "Topic": "beepul.transfer.events",
      "DeadLetterTopic": "beepul.transfer.events.dlq",
      "MaxBatchSize": 50000,
      "InitialBatchCapacity": 50000,
      "MaxBatchBytes": 67108864,
      "BatchTimeout": "00:00:03",
      "ConsumePollInterval": "00:00:00.200",
      "ProcessingPollInterval": "00:00:01",
      "AutoOffsetReset": "Earliest",
      "PartitionAssignmentStrategy": "CooperativeSticky",
      "MaxRetryAttempts": 0,
      "InitialRetryDelay": "00:00:00.500",
      "MaxRetryDelay": "00:00:30",
      "RetryBackoffMultiplier": 2,
      "MaxPollInterval": "00:15:00",
      "CommitRetryDelay": "00:00:01",
      "DeadLetterAcks": "All",
      "DeadLetterEnableIdempotence": true,
      "DeadLetterMessageTimeoutMs": 30000,
      "DeadLetterFlushTimeout": "00:00:10"
    }
  }
}
```

## 15. Demo load generator options

Demo ilovada alohida bo‘lim mavjud:

```json
"LoadGenerator": {
  "Topic": "beepul.transfer.events",
  "TransfersPerSecond": 10000,
  "MaxDurationSeconds": 60
}
```

### Topic

Load generator event yuboradigan topic. Consumer `Kafka:Consumer:Topic` bilan bir xil bo‘lishi kerak.

### TransfersPerSecond

Bir sekundlik intervalda yaratiladigan transferlar soni.

Har transfer uchun uch event yaratilgani sabab:

```text
EventsPerSecond = TransfersPerSecond × 3
```

`10000` transfer/s:

```text
10 000 created
10 000 authorized
10 000 paid
= 30 000 event/sekund
```

### MaxDurationSeconds

API orqali bitta load generation request uchun ruxsat etilgan maksimal `seconds` qiymati. Juda uzun tasodifiy testlardan himoya qiladi.

## 16. Demo API’dan foydalanish

Kafka’ni tekshirish:

```bash
nc -vz localhost 9092
```

Demo’ni ishga tushirish:

```bash
dotnet run \
  --project /home/t_ergashev/projects/Kafka.Demo/Kafka.Demo/Kafka.Demo.csproj
```

10 sekundlik test:

```bash
curl -X POST \
  "http://localhost:5122/api/transfers/publish?seconds=10"
```

`TransfersPerSecond=10000` bo‘lsa:

```text
100 000 transfer
300 000 event
```

Consumer logi:

```text
Batch qabul qilindi va skip qilindi.
EventCount=50000, TransferCount=16668
```

Batch transfer chegarasida kesilmasligi mumkin. Shu sabab bitta transferning `created/authorized` eventlari bir batchda, `paid` eventi keyingi batchda bo‘lishi mumkin. Kafka partition ichidagi tartib saqlanadi.

## 17. Topic, partition, broker va replication

### Lokal test

```text
1 broker
6 partition
replication factor 1
```

Bu throughput va consumer flow testiga yetadi, lekin broker o‘lsa cluster unavailable bo‘ladi.

### Production boshlang‘ich tavsiyasi

```text
3 broker
12 partition (real benchmark bilan tasdiqlanadi)
replication factor 3
min.insync.replicas 2
producer acks All
```

Partition soni consumer parallelizmini cheklaydi:

```text
12 partition → bir consumer groupda maksimal 12 faol consumer
```

Partition soni formulasi:

```text
P >= max(
  target produce throughput / measured partition throughput,
  target consume throughput / measured consumer worker throughput,
  kerakli consumer instance soni
)
```

Ustiga growth va failure uchun 30–100% zaxira qo‘shiladi.

## 18. ClickHouse handler talablari

Handler quyidagilarga amal qilishi kerak:

1. `EventId` bo‘yicha idempotent insert.
2. Bitta batch uchun bulk insert.
3. Temporary ClickHouse xatosida oddiy exception tashlash.
4. Cancellation tokenni barcha async chaqiriqlarga uzatish.
5. Success bo‘lmagan holatda exceptionni yutib yubormaslik.
6. Eski payloadlarda yangi nullable/default fieldlarni qabul qilish.
7. ClickHouse schema migrationni producer deployidan oldin bajarish.

Noto‘g‘ri handler:

```csharp
try
{
    await Insert(batch);
}
catch (Exception exception)
{
    logger.LogError(exception, "Insert failed");
    // Exception yutildi — package buni success deb o‘ylab offsetni commit qiladi.
}
```

To‘g‘ri handler:

```csharp
try
{
    await Insert(batch, cancellationToken);
}
catch (Exception exception)
{
    logger.LogError(exception, "Insert failed");
    throw;
}
```

## 19. Tavsiya etiladigan release tartibi

Payloadga yangi optional field qo‘shilganda:

```text
1. ClickHouse’ga nullable/default column qo‘shish
2. Consumer’ni yangi fieldni qabul qiladigan qilib deploy qilish
3. Producer’ni yangi field yuboradigan qilib deploy qilish
```

Bu eski va yangi JSON’lar bir batchda kelganda compatibility’ni saqlaydi.

## 20. Monitoring

Production’da kamida quyidagilar kuzatilishi kerak:

- producer delivery error count;
- producer throughput event/s va MB/s;
- consumer lag har partition bo‘yicha;
- batch event count va byte size;
- batch yig‘ish vaqti;
- ClickHouse insert duration;
- retry count va joriy retry delay;
- DLQ event count;
- commit error count;
- process memory, GC va CPU;
- broker disk, network va under-replicated partitionlar.

## 21. Keng tarqalgan xatolar

### `Connection refused localhost:9092`

Kafka broker ishlamayapti yoki boshqa portda:

```bash
nc -vz localhost 9092
```

### API Docker ichida, Kafka boshqa containerda

Docker ichida `localhost` shu containerning o‘zi. Docker network service nomini ishlatish kerak:

```json
"BootstrapServers": "kafka:9092"
```

### Consumer eski eventlarni o‘qimayapti

Sabablar:

- group committed offset oxirida;
- `AutoOffsetReset` faqat committed offset yo‘q bo‘lganda ishlaydi;
- yangi test uchun yangi `GroupId` kerak bo‘lishi mumkin.

### ClickHouse xatosiga qaramay event commit bo‘ldi

Handler exceptionni catch qilib yutib yuborgan bo‘lishi mumkin. Handler xatoni qayta `throw` qilishi shart.

### Consumer DLQ xatosida to‘xtadi

Bu data loss oldini olish uchun ataylab qilingan. DLQ topic, broker availability va `DeadLetterTopic` nomini tekshirish kerak.

## 22. Muhim yakuniy qoidalar

```text
1. EventId retrylarda o‘zgarmaydi.
2. Tartib kerak bo‘lsa entity ID Kafka key bo‘ladi.
3. Handler success bo‘lmasa offset commit qilinmaydi.
4. ClickHouse handler EventId bo‘yicha idempotent bo‘ladi.
5. MaxRetryAttempts=0 temporary storage failure uchun eng xavfsiz rejim.
6. DLQ delivery tasdiqlanmasa original record commit qilinmaydi.
7. Batch count, bytes va timeout birgalikda sozlanadi.
8. Partition va consumer soni real ClickHouse benchmark bilan tanlanadi.
9. Production’da kamida 3 broker, RF=3, minISR=2 va Acks=All tavsiya qilinadi.
10. Payload o‘zgarishlari backward-compatible bo‘lishi kerak.
```
