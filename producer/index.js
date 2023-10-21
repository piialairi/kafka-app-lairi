import { Kafka, Partitioners } from 'kafkajs'
import { v4 as UUID } from 'uuid';
console.log("*** Producer Starts... ***");

const kafka = new Kafka({
    clientId: 'my-checking-client',
    brokers: ['localhost:9092']
});

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const responseProducer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner }); // Create a producer for 'convertedresult'
const consumer = kafka.consumer({ groupId: 'my-group' }); // Create a consumer for 'tobechecked'
const run = async () => {
    //producing
    await producer.connect();
    await responseProducer.connect(); // Connect the response producer
    await consumer.connect();
    await consumer.subscribe({ topic: 'tobechecked' }); // Subscribe to 'tobechecked' topic

    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            // Convert Fahrenheit to Celsius
            const fahrenheit = parseFloat(message.value.toString());
            const celsius = (fahrenheit - 32) * (5 / 9);
            const uuidFraction = message.key;

            // Produce the response to 'convertedresult'
            await responseProducer.send({
                topic: 'convertedresult',
                messages: [
                    {
                        key: uuidFraction,
                        value: Buffer.from(JSON.stringify({ celsius, uuid: uuidFraction })),
                    },
                ],
            });

            console.log(`Message ${uuidFraction} converted: ${fahrenheit}°F to ${celsius}°C`);
        },
    });

    // Produce random Fahrenheit numbers
    setInterval(() => {
        queueMessage();
    }, 2500);
}

async function queueMessage() {
    const uuidFraction = UUID().substring(0, 4);
    const fahrenheit = randomizeIntegerBetween(0, 100); // Replace with your desired range

    const success = await producer.send({
        topic: 'tobechecked',
        messages: [
            {
                key: uuidFraction,
                value: Buffer.from(fahrenheit.toString()),
            },
        ],
    });

    if (success) {
        console.log(`Message ${uuidFraction} sent to the stream: ${fahrenheit}°F`);
    } else {
        console.log('Problem writing to stream...');
    }
}

function randomizeIntegerBetween(from, to) {
    return Math.floor(Math.random() * (to - from + 1)) + from;
}

run().catch(console.error);