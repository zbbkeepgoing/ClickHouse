#pragma once

#include <Processors/ISource.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>
#include <Storages/RabbitMQ/RabbitMQConsumer.h>


namespace DB
{

class RabbitMQSource : public ISource
{

public:
    RabbitMQSource(
            StorageRabbitMQ & storage_,
            const StorageSnapshotPtr & storage_snapshot_,
            ContextPtr context_,
            const Names & columns,
            size_t max_block_size_,
            bool ack_in_suffix = false);

    ~RabbitMQSource() override;

    String getName() const override { return storage.getName(); }
    RabbitMQConsumerPtr getBuffer() { return consumer; }

    Chunk generate() override;

    bool queueEmpty() const { return !consumer || consumer->queueEmpty(); }
    bool needChannelUpdate();
    void updateChannel();
    bool sendAck();


    void setTimeLimit(Poco::Timespan max_execution_time_) { max_execution_time = max_execution_time_; }

private:
    StorageRabbitMQ & storage;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    Names column_names;
    const size_t max_block_size;
    bool ack_in_suffix;

    bool is_finished = false;
    const Block non_virtual_header;
    const Block virtual_header;

    RabbitMQConsumerPtr consumer;

    Poco::Timespan max_execution_time = 0;
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};

    bool checkTimeLimit() const;

    RabbitMQSource(
        StorageRabbitMQ & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        std::pair<Block, Block> headers,
        ContextPtr context_,
        const Names & columns,
        size_t max_block_size_,
        bool ack_in_suffix);

    Chunk generateImpl();
};

}
