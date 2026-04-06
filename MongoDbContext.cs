// <copyright file="MongoDbContext.cs" company="Wowzer">
// Copyright(c) 2016-2017 Alexandre Spieser under the MIT License (MIT) 
// </copyright>

using System;
using System.Linq;
using System.Reflection;
using MongoDB.Bson;
using MongoDB.Driver;
using Wowzer.Services.Models.Mongo;
using Wowzer.Services.Mongo.Interfaces;

namespace Wowzer.Services.Mongo
{
    /// <inheritdoc />
    public class MongoDbContext : IMongoDbContext
    {
        /// <summary>
        ///     Initializes static members of the <see cref="MongoDbContext" /> class.
        /// </summary>
        static MongoDbContext()
        {
            // Avoid legacy UUID representation: use Binary 0x04 subtype.
            MongoDefaults.GuidRepresentation = GuidRepresentation.Standard;
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="MongoDbContext" /> class.
        /// </summary>
        /// <param name="mongoDatabase">The mongo database.</param>
        public MongoDbContext(IMongoDatabase mongoDatabase)
        {
            Database = mongoDatabase;
            Client = Database.Client;
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="MongoDbContext" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="databaseName">Name of the database.</param>
        public MongoDbContext(string connectionString, string databaseName)
        {
            Client = new MongoClient(connectionString);
            Database = Client.GetDatabase(databaseName);
        }

        /// <inheritdoc />
        public IMongoClient Client { get; }

        /// <inheritdoc />
        public IMongoDatabase Database { get; }

        /// <inheritdoc />
        public void SetObjectIdRepresentation(GuidRepresentation objectIdRepresentation)
        {
            MongoDefaults.GuidRepresentation = objectIdRepresentation;
        }

        /// <inheritdoc />
        public IMongoCollection<T> GetCollection<T>(string partitionKey = null) where T : IDocument
        {
            return string.IsNullOrEmpty(partitionKey)
                ? Database.GetCollection<T>(GetAttributeCollectionName<T>())
                : Database.GetCollection<T>(partitionKey + "-" + GetAttributeCollectionName<T>());
        }

        /// <inheritdoc />
        public IMongoCollection<T> GetCollection<T, TKey>(string partitionKey)
            where T : IDocument<TKey>
            where TKey : IEquatable<TKey>
        {
            return string.IsNullOrEmpty(partitionKey)
                ? Database.GetCollection<T>(GetAttributeCollectionName<T>())
                : Database.GetCollection<T>(partitionKey + "-" + GetAttributeCollectionName<T>());
        }

        /// <inheritdoc />
        public void DropCollection<T>()
        {
            Database.DropCollection(GetAttributeCollectionName<T>());
        }

        /// <inheritdoc />
        public void DropCollection<T>(string partitionKey)
        {
            Database.DropCollection(partitionKey + "-" + GetAttributeCollectionName<T>());
        }

        /// <summary>
        ///     Extracts the CollectionName attribute from the entity type, if any.
        /// </summary>
        /// <typeparam name="T">The type representing a Document.</typeparam>
        /// <returns>The name of the collection in which the T is stored.</returns>
        private static string GetAttributeCollectionName<T>()
        {
            return (typeof(T).GetTypeInfo()
                .GetCustomAttributes(typeof(CollectionName))
                .FirstOrDefault() as CollectionName)?.Name;
        }
    }
}
