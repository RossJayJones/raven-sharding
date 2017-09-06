using System;
using System.Collections.Generic;
using System.Transactions;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;

namespace Sharding35
{
    class Program
    {
        static void Main(string[] args)
        {
            NormalDocumentStore("x");
            ShardedDocumentStoreWithoutTransaction("y");
            ShardedDocumentStoreWithTransaction("z");

            var store = new DocumentStore { Url = "http://localhost:8092" };
            store.Initialize();

            using (var session = store.OpenSession("tenant"))
            {
                AssertNotNull(session.Load<Customer>("x"));
                AssertNotNull(session.Load<Customer>("Africa/y"));
                AssertNotNull(session.Load<Customer>("Africa/z"));
            }
        }

        static void AssertNotNull(object value)
        {
            if (value == null)
            {
                throw new InvalidOperationException("Document cannot be null");
            }
        }

        /// <summary>
        /// When creating a normal document store the transaction commits and saves the doc
        /// </summary>
        static void NormalDocumentStore(string id)
        {
            var store = new DocumentStore { Url = "http://localhost:8092" };
            store.Initialize();
            using (var scope = new TransactionScope())
            {
                using (var session = store.OpenSession("tenant"))
                {
                    var customer = new Customer
                    {
                        Id = id,
                        Name = "Customer 1",
                        Region = "Africa"
                    };
                    session.Store(customer);
                    session.SaveChanges();
                }

                scope.Complete();
            }
        }

        /// <summary>
        /// When creating a document using sharded document store without transaction everything works
        /// </summary>
        /// <param name="id"></param>
        static void ShardedDocumentStoreWithoutTransaction(string id)
        {
            var shards = new Dictionary<string, IDocumentStore>
            {
                {"Africa", new DocumentStore {Url = "http://localhost:8092"}},
            };

            var shardStrategy = new ShardStrategy(shards).ShardingOn<Customer>(company => company.Region);

            var store = new ShardedDocumentStore(shardStrategy).Initialize();

            store.Initialize();

            using (var session = store.OpenSession("tenant"))
            {
                var customer = new Customer
                {
                    Id = id,
                    Name = "Customer 1",
                    Region = "Africa"
                };
                session.Store(customer);
                session.SaveChanges();
            }
        }

        /// <summary>
        /// When creating a document using sharded document store with transactions the
        /// transaction does not commit
        /// </summary>
        /// <param name="id"></param>
        static void ShardedDocumentStoreWithTransaction(string id)
        {
            var shards = new Dictionary<string, IDocumentStore>
            {
                {"Africa", new DocumentStore {Url = "http://localhost:8092"}},
            };

            var shardStrategy = new ShardStrategy(shards).ShardingOn<Customer>(company => company.Region);

            var store = new ShardedDocumentStore(shardStrategy).Initialize();

            store.Initialize();

            using (var scope = new TransactionScope())
            {
                using (var session = store.OpenSession("tenant"))
                {
                    var customer = new Customer
                    {
                        Id = id,
                        Name = "Customer 1",
                        Region = "Africa"
                    };
                    session.Store(customer);
                    session.SaveChanges();
                }

                scope.Complete();
            }
        }
    }
}
