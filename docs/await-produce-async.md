# الإرسال المباشر إلى Kafka بـ await ProduceAsync

## الخطوات

1. أنشئ `IProducer` واحداً كـ Singleton يشاركه كل الطلبات، حتى تجتمع رسائلها في batches.
2. اضبط المنتج: `acks=all`، `enable.idempotence=true`، `linger.ms=5`، `message.timeout.ms=5000`.
3. اضبط الـ topic: `replication.factor=3` و`min.insync.replicas=2`.
4. داخل الطلب، نفّذ `await ProduceAsync` بمفتاح ثابت هو `id` الرسالة.
5. عند التأكيد، أرجع `202 Accepted`.
6. عند timeout أو امتلاء الطابور، أرجع `503`، فالرسالة لم تُقبل.
7. عند إيقاف الخدمة، نفّذ `producer.Flush()` لإكمال ما في الذاكرة.
8. عند المستهلك، تجاهل أي `id` سبق تسجيله، لأن فقدان التأكيد قد يسبب تكراراً.

## Sequence Diagram

```mermaid
sequenceDiagram
    participant C as Client
    participant A as WebAPI
    participant K as Kafka (RF=3, ISR>=2)
    C->>A: POST /orders {id: 42}
    A->>K: ProduceAsync(key=42), acks=all
    alt Written to 2+ replicas
        K-->>A: Delivered, offset=1001
        A-->>C: 202 Accepted
    else Timeout after 5s or queue full
        K--xA: ProduceException
        A-->>C: 503, message not accepted
    end
```
*لاحظ: 202 لا يصل إلا بعد تأكيد Kafka.*

## Class Diagram

```mermaid
classDiagram
    class MessagesController {
        +Post(request, ct) IActionResult
    }
    class KafkaMessagePublisher {
        +PublishAsync(id, json, ct) Task
    }
    class IProducer {
        <<interface>>
        +ProduceAsync(topic, message, ct) Task
        +Flush(timeout) int
    }
    MessagesController --> KafkaMessagePublisher : calls per request
    KafkaMessagePublisher --> IProducer : shares one singleton
```
*لاحظ: يوجد منتج واحد لكل الطلبات، وهو أساس الـ batching.*

## Flowchart

```mermaid
flowchart TD
    A[Request id=42] --> B[await ProduceAsync]
    B --> C{Kafka acknowledged?}
    C -- Yes --> D[Return 202]
    C -- Timeout or queue full --> E[Return 503]
    D --> F[Consumer checks id]
    F --> G{id already recorded?}
    G -- No --> H[Record message]
    G -- Yes --> I[Ignore duplicate]
    style D fill:#9f9,color:#000
    style E fill:#f99,color:#000
```
*لاحظ: كل مسار ينتهي برد واضح، فلا يوجد نجاح بلا تأكيد.*

## مثال

الافتراض: 5,000 رسالة/ثانية، و`linger.ms=5`. الأرقام محسوبة، وليست قياساً على Kafka.

```
حجم الدفعة   = 5,000 × 0.005s = 25 رسالة
عدد الدفعات  = 5,000 ÷ 25     = 200 دفعة/ثانية
```

| الحدث | الرسائل المؤكدة المفقودة | ما يراه العميل |
|---|---|---|
| Crash للـ API | 0 | خطأ اتصال للطلبات المعلقة |
| تعطل broker واحد | 0 | زمن أعلى لفترة قصيرة أثناء تبديل الـ leader |
| تعطل brokerين من 3 | 0 | `503` |
| ❌ `Produce` بلا فحص النتيجة، مع 100ms في الذاكرة | حتى 500 (5,000 × 0.1) | `202` لرسائل ضاعت |

## تطبيقات مشابهة

- **Confluent Cloud (الولايات المتحدة):** توثيقه يوصي بنفس ضبط المتانة: `RF=3` و`min.insync.replicas=2` و`acks=all`. [المرجع](https://docs.confluent.io/cloud/current/client-apps/optimizing/durability.html)
