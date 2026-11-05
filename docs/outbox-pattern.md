# Outbox Pattern

## الخطوات

1. أنشئ جدول `outbox` في نفس قاعدة بيانات العمل، بالأعمدة: `id`، `topic`، `key`، `payload`، `created_at`.
2. افتح transaction واحدة، واكتب فيها بيانات العمل وصف الـ `outbox` معاً.
3. نفّذ `COMMIT`، ثم أرجع `202` للعميل.
4. انقل صفوف الـ `outbox` إلى Kafka عبر Relay، إما بـ polling أو عبر CDC مثل Debezium الذي يقرأ log قاعدة البيانات.
5. أرسل بإعدادات `acks=all` و`enable.idempotence=true`.
6. بعد تأكيد Kafka، علّم الصف كمرسل أو احذفه.
7. احذف الصفوف القديمة دورياً، أو قسّم الجدول إلى partitions زمنية.
8. عند المستهلك، تجاهل أي `id` سبق تسجيله، لأن انهيار Relay قبل الخطوة 6 يعيد الإرسال.

## Sequence Diagram

```mermaid
sequenceDiagram
    participant C as Client
    participant A as WebAPI
    participant D as DB
    participant R as Relay
    participant K as Kafka
    C->>A: POST /orders {id: 42}
    A->>D: BEGIN, INSERT order, INSERT outbox, COMMIT
    D-->>A: Both saved, or neither
    A-->>C: 202 Accepted
    R->>D: Read unsent rows
    R->>K: Produce(id=42), acks=all
    K-->>R: Delivered
    R->>D: Mark id=42 as sent
    Note over R,D: Crash before marking means resend, so the consumer dedupes
```
*لاحظ: قيمة النمط في الـ transaction المشتركة، لا في مكان التخزين.*

## Class Diagram

```mermaid
classDiagram
    class OrdersController {
        +Post(request, ct) IActionResult
    }
    class OrderService {
        +CreateAsync(order, ct) Task
    }
    class OutboxMessage {
        +Guid Id
        +string Topic
        +string Key
        +string Payload
        +DateTimeOffset CreatedAt
        +DateTimeOffset SentAt
    }
    class OutboxRelayWorker {
        +ExecuteAsync(ct) Task
    }
    OrdersController --> OrderService : calls
    OrderService --> OutboxMessage : saves in same transaction
    OutboxRelayWorker --> OutboxMessage : reads and marks sent
```
*لاحظ: `OrderService` وحده يكتب الصف، و`OutboxRelayWorker` وحده يرسله.*

## Flowchart

```mermaid
flowchart TD
    A[Request id=42] --> B[BEGIN transaction]
    B --> C[INSERT order + outbox]
    C --> D{COMMIT succeeded?}
    D -- No --> E[Return error, nothing saved]
    D -- Yes --> F[Return 202]
    F --> G[Relay reads row]
    G --> H{Kafka acknowledged?}
    H -- No --> G
    H -- Yes --> I[Mark row as sent]
    style D fill:#fd9,color:#000
```
*لاحظ: `COMMIT` هو نقطة القرار الوحيدة، وبعدها يبقى Relay يعيد المحاولة حتى ينجح.*

## مثال

الافتراض: 5,000 رسالة/ثانية، و1KB للرسالة. الأرقام محسوبة.

| البند | القيمة |
|---|---|
| كتابات الجدول | 5,000 `INSERT`/ثانية إضافية |
| النمو اليومي | 5,000 × 86,400 = 432 مليون صف |
| حجم البيانات اليومي | 432GB قبل الفهارس |

الفرق عند انهيار السيرفر بين الخطوتين:

```
بدون Outbox:
t=0  COMMIT الطلب 42    ✅
t=1  crash              💥
t=2  الإرسال إلى Kafka  لم يحدث
t=3  الطلب محفوظ، والرسالة لن تصل أبداً

مع Outbox:
t=0  COMMIT الطلب 42 + صف outbox  ✅
t=1  crash                        💥
t=2  إعادة التشغيل، وRelay يقرأ الصف
t=3  الرسالة تصل إلى Kafka        ✅
```

## تطبيقات مشابهة

- **Clear Street (الولايات المتحدة):** بنت Outbox فوق Debezium وKafka، وتصف تشغيله في الإنتاج. [المرجع](https://www.clearstreet.io/insights/transactional-outboxes)
- **SeatGeek (الولايات المتحدة):** تشرح تصميمها للنمط باستخدام Outbox Event Router من Debezium. [المرجع](https://chairnerd.seatgeek.com/transactional-outbox-pattern)
