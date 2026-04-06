// <copyright file="BaseMongoRepository.cs" company="Wowzer">
// Copyright(c) 2016-2017 Alexandre Spieser under the MIT License (MIT) 
// </copyright>

using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Wowzer.Services.Models.Mongo;
using Wowzer.Services.Mongo.Interfaces;

namespace Wowzer.Services.Mongo
{
     /// <summary>
     /// The base Repository, it is meant to be inherited from by your custom custom MongoRepository implementation.
     /// Its constructor must be given a connection string and a database name.
     /// </summary>
     public abstract class BaseMongoRepository : IBaseMongoRepository
     {
          /// <summary>
          /// Initializes a new instance of the <see cref="BaseMongoRepository"/> class.
          /// </summary>
          /// <param name="connectionString">The connection string.</param>
          /// <param name="databaseName">Name of the database.</param>
          protected BaseMongoRepository(string connectionString, string databaseName)
          {
               MongoDbContext = new MongoDbContext(connectionString, databaseName);
          }

          /// <summary>
          /// Initializes a new instance of the <see cref="BaseMongoRepository"/> class.
          /// </summary>
          /// <param name="mongoDbContext">The mongo database context.</param>
          protected BaseMongoRepository(IMongoDbContext mongoDbContext)
          {
               MongoDbContext = mongoDbContext;
          }

          /// <summary>
          /// Initializes a new instance of the <see cref="BaseMongoRepository"/> class.
          /// </summary>
          /// <param name="mongoDatabase">The mongo database.</param>
          protected BaseMongoRepository(IMongoDatabase mongoDatabase)
          {
               MongoDbContext = new MongoDbContext(mongoDatabase);
          }

          /// <inheritdoc />
          public string ConnectionString { get; set; }

          /// <inheritdoc />
          public string DatabaseName { get; set; }

          /// <summary>
          /// Gets or sets the mongo database context.
          /// </summary>
          /// <value>
          /// The mongo database context.
          /// </value>
          public IMongoDbContext MongoDbContext { get; set; }

          #region Read

          /// <inheritdoc />
          public async Task<TDocument> GetByIdAsync<TDocument>(ObjectId id, string partitionKey = null) where TDocument : IDocument
          {
               var filter = Builders<TDocument>.Filter.Eq("Id", id);
               return await HandlePartitioned<TDocument>(partitionKey).Find(filter).FirstOrDefaultAsync();
          }

          /// <inheritdoc />
          public TDocument GetById<TDocument>(ObjectId id, string partitionKey = null) where TDocument : IDocument
          {
               var filter = Builders<TDocument>.Filter.Eq("Id", id);
               return HandlePartitioned<TDocument>(partitionKey).Find(filter).FirstOrDefault();
          }

          /// <inheritdoc />
          public async Task<TDocument> GetOneAsync<TDocument>(Expression<Func<TDocument, bool>> filter, string partitionKey = null) where TDocument : IDocument
          {
               return await HandlePartitioned<TDocument>(partitionKey).Find(filter).FirstOrDefaultAsync();
          }

          /// <inheritdoc />
          public TDocument GetOne<TDocument>(Expression<Func<TDocument, bool>> filter, string partitionKey = null) where TDocument : IDocument
          {
               return HandlePartitioned<TDocument>(partitionKey).Find(filter).FirstOrDefault();
          }

          /// <inheritdoc />
          public IFindFluent<TDocument, TDocument> GetCursor<TDocument>(Expression<Func<TDocument, bool>> filter, string partitionKey = null) where TDocument : IDocument
          {
               return HandlePartitioned<TDocument>(partitionKey).Find(filter);
          }

          /// <inheritdoc />
          public async Task<bool> AnyAsync<TDocument>(Expression<Func<TDocument, bool>> filter, string partitionKey = null) where TDocument : IDocument
          {
               var count = await HandlePartitioned<TDocument>(partitionKey).CountAsync(filter);
               return count > 0;
          }

          /// <inheritdoc />
          public bool Any<TDocument>(Expression<Func<TDocument, bool>> filter, string partitionKey = null) where TDocument : IDocument
          {
               var count = HandlePartitioned<TDocument>(partitionKey).Count(filter);
               return count > 0;
          }

          /// <inheritdoc />
          public async Task<List<TDocument>> GetAllAsync<TDocument>(Expression<Func<TDocument, bool>> filter, string partitionKey = null) where TDocument : IDocument
          {
               return await HandlePartitioned<TDocument>(partitionKey).Find(filter).ToListAsync();
          }

          /// <inheritdoc />
          public List<TDocument> GetAll<TDocument>(Expression<Func<TDocument, bool>> filter, string partitionKey = null) where TDocument : IDocument
          {
               return HandlePartitioned<TDocument>(partitionKey).Find(filter).ToList();
          }

          /// <inheritdoc />
          public async Task<long> CountAsync<TDocument>(Expression<Func<TDocument, bool>> filter, string partitionKey = null) where TDocument : IDocument
          {
               return await HandlePartitioned<TDocument>(partitionKey).CountAsync(filter);
          }

          /// <inheritdoc />
          public long Count<TDocument>(Expression<Func<TDocument, bool>> filter, string partitionKey = null) where TDocument : IDocument
          {
               return HandlePartitioned<TDocument>(partitionKey).Find(filter).Count();
          }

          #endregion

          #region Read TKey

          /// <inheritdoc />
          public async Task<TDocument> GetByIdAsync<TDocument, TKey>(TKey id, string partitionKey = null)
              where TDocument : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var filter = Builders<TDocument>.Filter.Eq("Id", id);
               return await HandlePartitioned<TDocument, TKey>(partitionKey).Find(filter).FirstOrDefaultAsync();
          }

          /// <inheritdoc />
          public TDocument GetById<TDocument, TKey>(TKey id, string partitionKey = null)
              where TDocument : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var filter = Builders<TDocument>.Filter.Eq("Id", id);
               return HandlePartitioned<TDocument, TKey>(partitionKey).Find(filter).FirstOrDefault();
          }

          /// <inheritdoc />
          public async Task<TDocument> GetOneAsync<TDocument, TKey>(Expression<Func<TDocument, bool>> filter, string partitionKey = null)
              where TDocument : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               return await HandlePartitioned<TDocument, TKey>(partitionKey).Find(filter).FirstOrDefaultAsync();
          }

          /// <inheritdoc />
          public TDocument GetOne<TDocument, TKey>(Expression<Func<TDocument, bool>> filter, string partitionKey = null)
              where TDocument : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               return HandlePartitioned<TDocument, TKey>(partitionKey).Find(filter).FirstOrDefault();
          }

          /// <inheritdoc />
          public IFindFluent<TDocument, TDocument> GetCursor<TDocument, TKey>(Expression<Func<TDocument, bool>> filter, string partitionKey = null)
              where TDocument : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               return HandlePartitioned<TDocument, TKey>(partitionKey).Find(filter);
          }

          /// <inheritdoc />
          public async Task<bool> AnyAsync<TDocument, TKey>(Expression<Func<TDocument, bool>> filter, string partitionKey = null)
              where TDocument : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var count = await HandlePartitioned<TDocument, TKey>(partitionKey).CountAsync(filter);
               return count > 0;
          }

          /// <inheritdoc />
          public bool Any<TDocument, TKey>(Expression<Func<TDocument, bool>> filter, string partitionKey = null)
              where TDocument : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var count = HandlePartitioned<TDocument, TKey>(partitionKey).Count(filter);
               return count > 0;
          }

          /// <inheritdoc />
          public async Task<List<TDocument>> GetAllAsync<TDocument, TKey>(Expression<Func<TDocument, bool>> filter, string partitionKey = null)
              where TDocument : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               return await HandlePartitioned<TDocument, TKey>(partitionKey).Find(filter).ToListAsync();
          }

          /// <inheritdoc />
          public List<TDocument> GetAll<TDocument, TKey>(Expression<Func<TDocument, bool>> filter, string partitionKey = null)
              where TDocument : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               return HandlePartitioned<TDocument, TKey>(partitionKey).Find(filter).ToList();
          }

          /// <inheritdoc />
          public async Task<long> CountAsync<TDocument, TKey>(Expression<Func<TDocument, bool>> filter, string partitionKey = null)
              where TDocument : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               return await HandlePartitioned<TDocument, TKey>(partitionKey).CountAsync(filter);
          }

          /// <inheritdoc />
          public long Count<TDocument, TKey>(Expression<Func<TDocument, bool>> filter, string partitionKey = null)
              where TDocument : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               return HandlePartitioned<TDocument, TKey>(partitionKey).Find(filter).Count();
          }

          #endregion

          #region Create

          /// <inheritdoc />
          public virtual async Task AddOneAsync<T>(T document) where T : IDocument
          {
               FormatDocument(document);
               await HandlePartitioned(document).InsertOneAsync(document);
          }

          /// <inheritdoc />
          public virtual void AddOne<T>(T document) where T : IDocument
          {
               FormatDocument(document);
               HandlePartitioned(document).InsertOne(document);
          }

          /// <inheritdoc />
          public virtual async Task AddManyAsync<T>(IEnumerable<T> documents) where T : IDocument
          {
               var enumerable = documents.ToList();
               if (!enumerable.Any())
               {
                    return;
               }

               foreach (var doc in enumerable)
               {
                    FormatDocument(doc);
               }

               await HandlePartitioned(enumerable.FirstOrDefault()).InsertManyAsync(enumerable);
          }

          /// <inheritdoc />
          public virtual void AddMany<T>(IEnumerable<T> documents) where T : IDocument
          {
               var enumerable = documents.ToList();
               if (!enumerable.Any())
               {
                    return;
               }

               foreach (var document in enumerable)
               {
                    FormatDocument(document);
               }

               HandlePartitioned(enumerable.FirstOrDefault()).InsertMany(enumerable.ToList());
          }

          #endregion Create

          #region Create TKey

          /// <inheritdoc />
          public virtual async Task AddOneAsync<T, TKey>(T document)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               FormatDocument<T, TKey>(document);
               await HandlePartitioned<T, TKey>(document).InsertOneAsync(document);
          }

          /// <inheritdoc />
          public virtual void AddOne<T, TKey>(T document)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               FormatDocument<T, TKey>(document);
               HandlePartitioned<T, TKey>(document).InsertOne(document);
          }

          /// <inheritdoc />
          public virtual async Task AddManyAsync<T, TKey>(IEnumerable<T> documents)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var enumerable = documents.ToList();
               if (!enumerable.Any())
               {
                    return;
               }

               foreach (var doc in enumerable)
               {
                    FormatDocument<T, TKey>(doc);
               }

               await HandlePartitioned<T, TKey>(enumerable.FirstOrDefault()).InsertManyAsync(enumerable);
          }

          /// <inheritdoc />
          public virtual void AddMany<T, TKey>(IEnumerable<T> documents)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var enumerable = documents.ToList();
               if (!enumerable.Any())
               {
                    return;
               }

               foreach (var document in enumerable)
               {
                    FormatDocument<T, TKey>(document);
               }

               HandlePartitioned<T, TKey>(enumerable.FirstOrDefault()).InsertMany(enumerable.ToList());
          }

          #endregion

          #region Update

          /// <inheritdoc />
          public virtual async Task<bool> UpdateOneAsync<T>(T modifiedDocument) where T : IDocument
          {
               var updateRes = await HandlePartitioned(modifiedDocument).ReplaceOneAsync(x => x.Id == modifiedDocument.Id, modifiedDocument);
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual bool UpdateOne<T>(T modifiedDocument) where T : IDocument
          {
               var updateRes = HandlePartitioned(modifiedDocument).ReplaceOne(x => x.Id == modifiedDocument.Id, modifiedDocument);
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual async Task<bool> UpdateOneAsync<T>(T documentToModify, UpdateDefinition<T> update)
              where T : IDocument
          {
               var filter = Builders<T>.Filter.Eq("Id", documentToModify.Id);
               var updateRes = await HandlePartitioned(documentToModify).UpdateOneAsync(filter, update);
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual bool UpdateOne<T, TField>(T documentToModify, Expression<Func<T, TField>> field, TField value)
              where T : IDocument
          {
               var filter = Builders<T>.Filter.Eq("Id", documentToModify.Id);
               var updateRes = HandlePartitioned(documentToModify).UpdateOne(filter, Builders<T>.Update.Set(field, value));
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual async Task<bool> UpdateOneAsync<T, TField>(T documentToModify, Expression<Func<T, TField>> field, TField value)
              where T : IDocument
          {
               var filter = Builders<T>.Filter.Eq("Id", documentToModify.Id);
               var updateRes = await HandlePartitioned(documentToModify).UpdateOneAsync(filter, Builders<T>.Update.Set(field, value));
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual bool UpdateOne<T, TField>(FilterDefinition<T> filter, Expression<Func<T, TField>> field, TField value, string partitionKey = null)
              where T : IDocument
          {
               var collection = string.IsNullOrEmpty(partitionKey) ? GetCollection<T>() : GetCollection<T>(partitionKey);
               var updateRes = collection.UpdateOne(filter, Builders<T>.Update.Set(field, value));
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual bool UpdateOne<T, TField>(Expression<Func<T, bool>> filter, Expression<Func<T, TField>> field, TField value, string partitionKey = null)
              where T : IDocument
          {
               var collection = string.IsNullOrEmpty(partitionKey) ? GetCollection<T>() : GetCollection<T>(partitionKey);
               var updateRes = collection.UpdateOne(Builders<T>.Filter.Where(filter), Builders<T>.Update.Set(field, value));
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual async Task<bool> UpdateOneAsync<T, TField>(FilterDefinition<T> filter, Expression<Func<T, TField>> field, TField value, string partitionKey = null)
              where T : IDocument
          {
               var collection = string.IsNullOrEmpty(partitionKey) ? GetCollection<T>() : GetCollection<T>(partitionKey);
               var updateRes = await collection.UpdateOneAsync(filter, Builders<T>.Update.Set(field, value));
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual async Task<bool> UpdateOneAsync<T>(FilterDefinition<T> filter, UpdateDefinition<T> update, string partitionKey = null)
              where T : IDocument
          {
               var collection = string.IsNullOrEmpty(partitionKey) ? GetCollection<T>() : GetCollection<T>(partitionKey);
               var updateRes = await collection.UpdateOneAsync(filter, update);
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual async Task<bool> UpdateOneAsync<T, TField>(Expression<Func<T, bool>> filter, Expression<Func<T, TField>> field, TField value, string partitionKey = null)
              where T : IDocument
          {
               var collection = string.IsNullOrEmpty(partitionKey) ? GetCollection<T>() : GetCollection<T>(partitionKey);
               var updateRes = await collection.UpdateOneAsync(Builders<T>.Filter.Where(filter), Builders<T>.Update.Set(field, value));
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual bool UpdateOne<T>(T documentToModify, UpdateDefinition<T> update) where T : IDocument
          {
               var filter = Builders<T>.Filter.Eq("Id", documentToModify.Id);
               var updateRes = HandlePartitioned(documentToModify).UpdateOne(filter, update, new UpdateOptions { IsUpsert = true });
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual async Task<bool> InsertOrUpdateAsync<T>(T documentToModify, UpdateDefinition<T> update) where T : IDocument
          {
               var filter = Builders<T>.Filter.Eq("Id", documentToModify.Id);
               var updateRes = await HandlePartitioned(documentToModify).UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = true });
               return updateRes.ModifiedCount == 1;
          }

          public virtual async Task<long> UpdateManyAsync<T, TField>(IEnumerable<T> documents, Expression<Func<T, TField>> field, TField value) where T : IDocument
          {

               var enumerable = documents.ToList();
               if (!enumerable.Any())
               {
                    return 0;
               }

               var idsToupdate = enumerable.Select(e => e.Id).ToArray();
               return (await HandlePartitioned(enumerable.FirstOrDefault()).UpdateManyAsync(x => idsToupdate.Contains(x.Id), Builders<T>.Update.Set(field, value))).ModifiedCount;
          }

          #endregion Update

          #region Update TKey

          /// <inheritdoc />
          public virtual async Task<bool> UpdateOneAsync<T, TKey>(T modifiedDocument)
            where T : IDocument<TKey>
            where TKey : IEquatable<TKey>
          {
               var filter = Builders<T>.Filter.Eq("Id", modifiedDocument.Id);
               var updateRes = await HandlePartitioned<T, TKey>(modifiedDocument).ReplaceOneAsync(filter, modifiedDocument);
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual bool UpdateOne<T, TKey>(T modifiedDocument)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var filter = Builders<T>.Filter.Eq("Id", modifiedDocument.Id);
               var updateRes = HandlePartitioned<T, TKey>(modifiedDocument).ReplaceOne(filter, modifiedDocument);
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual async Task<bool> UpdateOneAsync<T, TKey>(T documentToModify, UpdateDefinition<T> update)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var filter = Builders<T>.Filter.Eq("Id", documentToModify.Id);
               var updateRes = await HandlePartitioned<T, TKey>(documentToModify).UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = true });
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual bool UpdateOne<T, TKey>(T documentToModify, UpdateDefinition<T> update)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var filter = Builders<T>.Filter.Eq("Id", documentToModify.Id);
               var updateRes = HandlePartitioned<T, TKey>(documentToModify).UpdateOne(filter, update, new UpdateOptions { IsUpsert = true });
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual async Task<bool> UpdateOneAsync<T, TKey, TField>(T documentToModify, Expression<Func<T, TField>> field, TField value)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var filter = Builders<T>.Filter.Eq("Id", documentToModify.Id);
               var updateRes = await HandlePartitioned<T, TKey>(documentToModify).UpdateOneAsync(filter, Builders<T>.Update.Set(field, value));
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual bool UpdateOne<T, TKey, TField>(T documentToModify, Expression<Func<T, TField>> field, TField value)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var filter = Builders<T>.Filter.Eq("Id", documentToModify.Id);
               var updateRes = HandlePartitioned<T, TKey>(documentToModify).UpdateOne(filter, Builders<T>.Update.Set(field, value));
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual async Task<bool> UpdateOneAsync<T, TKey, TField>(FilterDefinition<T> filter, Expression<Func<T, TField>> field, TField value, string partitionKey = null)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var collection = string.IsNullOrEmpty(partitionKey) ? GetCollection<T, TKey>() : GetCollection<T, TKey>(partitionKey);
               var updateRes = await collection.UpdateOneAsync(filter, Builders<T>.Update.Set(field, value));
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual async Task<bool> UpdateOneAsync<T, TKey, TField>(Expression<Func<T, bool>> filter, Expression<Func<T, TField>> field, TField value, string partitionKey = null)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var collection = string.IsNullOrEmpty(partitionKey) ? GetCollection<T, TKey>() : GetCollection<T, TKey>(partitionKey);
               var updateRes = await collection.UpdateOneAsync(Builders<T>.Filter.Where(filter), Builders<T>.Update.Set(field, value));
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual bool UpdateOne<T, TKey, TField>(FilterDefinition<T> filter, Expression<Func<T, TField>> field, TField value, string partitionKey = null)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var collection = string.IsNullOrEmpty(partitionKey) ? GetCollection<T, TKey>() : GetCollection<T, TKey>(partitionKey);
               var updateRes = collection.UpdateOne(filter, Builders<T>.Update.Set(field, value));
               return updateRes.ModifiedCount == 1;
          }

          /// <inheritdoc />
          public virtual bool UpdateOne<T, TKey, TField>(Expression<Func<T, bool>> filter, Expression<Func<T, TField>> field, TField value, string partitionKey = null)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var collection = string.IsNullOrEmpty(partitionKey) ? GetCollection<T, TKey>() : GetCollection<T, TKey>(partitionKey);
               var updateRes = collection.UpdateOne(Builders<T>.Filter.Where(filter), Builders<T>.Update.Set(field, value));
               return updateRes.ModifiedCount == 1;
          }

          #endregion Update

          #region Delete

          /// <inheritdoc />
          public virtual async Task<long> DeleteOneAsync<T>(T document) where T : IDocument
          {
               return (await HandlePartitioned(document).DeleteOneAsync(x => x.Id == document.Id)).DeletedCount;
          }

          /// <inheritdoc />
          public virtual long DeleteOne<T>(T document) where T : IDocument
          {
               return HandlePartitioned(document).DeleteOne(x => x.Id == document.Id).DeletedCount;
          }

          /// <inheritdoc />
          public virtual long DeleteOne<T>(Expression<Func<T, bool>> filter, string partitionKey = null) where T : IDocument
          {
               return HandlePartitioned<T>(partitionKey).DeleteOne(filter).DeletedCount;
          }

          /// <inheritdoc />
          public virtual async Task<long> DeleteOneAsync<T>(Expression<Func<T, bool>> filter, string partitionKey = null) where T : IDocument
          {
               return (await HandlePartitioned<T>(partitionKey).DeleteOneAsync(filter)).DeletedCount;
          }

          /// <inheritdoc />
          public virtual async Task<long> DeleteManyAsync<T>(Expression<Func<T, bool>> filter, string partitionKey = null) where T : IDocument
          {
               return (await HandlePartitioned<T>(partitionKey).DeleteManyAsync(filter)).DeletedCount;
          }

          /// <inheritdoc />
          public virtual async Task<long> DeleteManyAsync<T>(IEnumerable<T> documents) where T : IDocument
          {
               var enumerable = documents.ToList();
               if (!enumerable.Any())
               {
                    return 0;
               }

               var idsTodelete = enumerable.Select(e => e.Id).ToArray();
               return (await HandlePartitioned(enumerable.FirstOrDefault()).DeleteManyAsync(x => idsTodelete.Contains(x.Id))).DeletedCount;
          }

          /// <inheritdoc />
          public virtual long DeleteMany<T>(IEnumerable<T> documents) where T : IDocument
          {
               var enumerable = documents.ToList();
               if (!enumerable.Any())
               {
                    return 0;
               }

               var idsTodelete = enumerable.Select(e => e.Id).ToArray();
               return HandlePartitioned(enumerable.FirstOrDefault()).DeleteMany(x => idsTodelete.Contains(x.Id)).DeletedCount;
          }

          /// <inheritdoc />
          public virtual long DeleteMany<T>(Expression<Func<T, bool>> filter, string partitionKey = null) where T : IDocument
          {
               return HandlePartitioned<T>(partitionKey).DeleteMany(filter).DeletedCount;
          }

          #endregion Delete

          #region Delete TKey

          /// <inheritdoc />
          public virtual long DeleteOne<T, TKey>(T document)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var filter = Builders<T>.Filter.Eq("Id", document.Id);
               return HandlePartitioned<T, TKey>(document).DeleteOne(filter).DeletedCount;
          }

          /// <inheritdoc />
          public virtual async Task<long> DeleteOneAsync<T, TKey>(T document)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var filter = Builders<T>.Filter.Eq("Id", document.Id);
               return (await HandlePartitioned<T, TKey>(document).DeleteOneAsync(filter)).DeletedCount;
          }

          /// <inheritdoc />
          public virtual long DeleteOne<T, TKey>(Expression<Func<T, bool>> filter, string partitionKey = null)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               return HandlePartitioned<T, TKey>(partitionKey).DeleteOne(filter).DeletedCount;
          }

          /// <inheritdoc />
          public virtual async Task<long> DeleteOneAsync<T, TKey>(Expression<Func<T, bool>> filter, string partitionKey = null)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               return (await HandlePartitioned<T, TKey>(partitionKey).DeleteOneAsync(filter)).DeletedCount;
          }

          /// <inheritdoc />
          public virtual async Task<long> DeleteManyAsync<T, TKey>(Expression<Func<T, bool>> filter, string partitionKey = null)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               return (await HandlePartitioned<T, TKey>(partitionKey).DeleteManyAsync(filter)).DeletedCount;
          }

          /// <inheritdoc />
          public virtual async Task<long> DeleteManyAsync<T, TKey>(IEnumerable<T> documents)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var enumerable = documents.ToList();
               if (!enumerable.Any())
               {
                    return 0;
               }

               var idsTodelete = enumerable.Select(e => e.Id).ToArray();
               return (await HandlePartitioned<T, TKey>(enumerable.FirstOrDefault()).DeleteManyAsync(x => idsTodelete.Contains(x.Id))).DeletedCount;
          }

          /// <inheritdoc />
          public virtual long DeleteMany<T, TKey>(IEnumerable<T> documents)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               var enumerable = documents.ToList();
               if (!enumerable.Any())
               {
                    return 0;
               }

               var idsTodelete = enumerable.Select(e => e.Id).ToArray();
               return HandlePartitioned<T, TKey>(enumerable.FirstOrDefault()).DeleteMany(x => idsTodelete.Contains(x.Id)).DeletedCount;
          }

          /// <inheritdoc />
          public virtual long DeleteMany<T, TKey>(Expression<Func<T, bool>> filter, string partitionKey = null)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               return HandlePartitioned<T, TKey>(partitionKey).DeleteMany(filter).DeletedCount;
          }

          #endregion

          #region Project

          /// <inheritdoc />
          public virtual async Task<TProjection> ProjectOneAsync<T, TProjection>(Expression<Func<T, bool>> filter, Expression<Func<T, TProjection>> projection, string partitionKey = null)
              where T : IDocument
              where TProjection : class
          {
               return await HandlePartitioned<T>(partitionKey).Find(filter)
                                                                      .Project(projection)
                                                                      .FirstOrDefaultAsync();
          }

          /// <inheritdoc />
          public virtual async Task<TProjection> ProjectOneAsync<T, TProjection, TKey>(Expression<Func<T, bool>> filter, Expression<Func<T, TProjection>> projection, string partitionKey = null)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
              where TProjection : class
          {
               return await HandlePartitioned<T, TKey>(partitionKey).Find(filter)
                                                                      .Project(projection)
                                                                      .FirstOrDefaultAsync();
          }

          /// <inheritdoc />
          public virtual TProjection ProjectOne<T, TProjection>(Expression<Func<T, bool>> filter, Expression<Func<T, TProjection>> projection, string partitionKey = null)
              where T : IDocument
              where TProjection : class
          {
               return HandlePartitioned<T>(partitionKey).Find(filter)
                                                                .Project(projection)
                                                                .FirstOrDefault();
          }

          /// <inheritdoc />
          public virtual TProjection ProjectOne<T, TProjection, TKey>(Expression<Func<T, bool>> filter, Expression<Func<T, TProjection>> projection, string partitionKey = null)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
              where TProjection : class
          {
               return HandlePartitioned<T, TKey>(partitionKey).Find(filter)
                                                                .Project(projection)
                                                                .FirstOrDefault();
          }

          /// <inheritdoc />
          public virtual async Task<List<TProjection>> ProjectManyAsync<T, TProjection>(Expression<Func<T, bool>> filter, Expression<Func<T, TProjection>> projection, string partitionKey = null)
              where T : IDocument
              where TProjection : class
          {
               return await HandlePartitioned<T>(partitionKey).Find(filter)
                                                                      .Project(projection)
                                                                      .ToListAsync();
          }

          /// <inheritdoc />
          public virtual async Task<List<TProjection>> ProjectManyAsync<T, TProjection, TKey>(Expression<Func<T, bool>> filter, Expression<Func<T, TProjection>> projection, string partitionKey = null)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
              where TProjection : class
          {
               return await HandlePartitioned<T, TKey>(partitionKey).Find(filter)
                                                                      .Project(projection)
                                                                      .ToListAsync();
          }

          /// <inheritdoc />
          public virtual List<TProjection> ProjectMany<T, TProjection>(Expression<Func<T, bool>> filter, Expression<Func<T, TProjection>> projection, string partitionKey = null)
              where T : IDocument
              where TProjection : class
          {
               return HandlePartitioned<T>(partitionKey).Find(filter)
                                                                .Project(projection)
                                                                .ToList();
          }

          /// <inheritdoc />
          public virtual List<TProjection> ProjectMany<T, TProjection, TKey>(Expression<Func<T, bool>> filter, Expression<Func<T, TProjection>> projection, string partitionKey = null)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
              where TProjection : class
          {
               return HandlePartitioned<T, TKey>(partitionKey).Find(filter)
                                                                .Project(projection)
                                                                .ToList();
          }

          #endregion

          #region Grouping

          /// <inheritdoc />
          public virtual List<TProjection> GroupBy<T, TGroupKey, TProjection>(
              Expression<Func<T, TGroupKey>> groupingCriteria,
              Expression<Func<IGrouping<TGroupKey, T>, TProjection>> groupProjection,
              string partitionKey = null)
              where T : IDocument
              where TProjection : class, new()
          {
               var collection = string.IsNullOrEmpty(partitionKey) ? GetCollection<T>() : GetCollection<T>(partitionKey);
               return collection.Aggregate()
                                .Group(groupingCriteria, groupProjection)
                                .ToList();
          }

          /// <inheritdoc />
          public virtual List<TProjection> GroupBy<T, TGroupKey, TProjection>(
                                                                          Expression<Func<T, bool>> filter,
                                                                          Expression<Func<T, TGroupKey>> selector,
                                                                          Expression<Func<IGrouping<TGroupKey, T>, TProjection>> projection,
                                                                          string partitionKey = null)
                                                                          where T : IDocument
                                                                          where TProjection : class, new()
          {
               var collection = string.IsNullOrEmpty(partitionKey) ? GetCollection<T>() : GetCollection<T>(partitionKey);
               return collection.Aggregate()
                                .Match(Builders<T>.Filter.Where(filter))
                                .Group(selector, projection)
                                .ToList();
          }

          #endregion

          #region GetPaginated

          /// <inheritdoc />
          public virtual async Task<List<T>> GetPaginatedAsync<T>(Expression<Func<T, bool>> filter, int skipNumber = 0, int takeNumber = 50, string partitionKey = null)
              where T : IDocument
          {
               return await HandlePartitioned<T>(partitionKey).Find(filter).Skip(skipNumber).Limit(takeNumber).ToListAsync();
          }

          /// <inheritdoc />
          public virtual async Task<List<T>> GetPaginatedAsync<T, TKey>(Expression<Func<T, bool>> filter, int skipNumber = 0, int takeNumber = 50, string partitionKey = null)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               return await HandlePartitioned<T, TKey>(partitionKey).Find(filter).Skip(skipNumber).Limit(takeNumber).ToListAsync();
          }

          public virtual async Task<List<T>> GetPaginatedAsync<T>(BsonDocument query, int skipNumber = 0, int takeNumber = 50, string partitionKey = null)
              where T : IDocument
          {
               return await HandlePartitioned<T>(partitionKey).Find(query).Skip(skipNumber).Limit(takeNumber).ToListAsync();
          }
          #endregion

          #region Find And Update

          /// <inheritdoc />
          public virtual async Task<T> GetAndUpdateOne<T>(FilterDefinition<T> filter, UpdateDefinition<T> update, FindOneAndUpdateOptions<T, T> options) where T : IDocument
          {
               return await GetCollection<T>().FindOneAndUpdateAsync(filter, update, options);
          }

          /// <inheritdoc />
          public virtual async Task<T> GetAndUpdateOne<T, TKey>(FilterDefinition<T> filter, UpdateDefinition<T> update, FindOneAndUpdateOptions<T, T> options)
              where T : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               return await GetCollection<T, TKey>().FindOneAndUpdateAsync(filter, update, options);
          }

          #endregion Find And Update

          #region Utility Methods

          /// <summary>
          /// Gets the collection.
          /// </summary>
          /// <typeparam name="TDocument">The type of the document.</typeparam>
          /// <param name="partitionKey">The partition key.</param>
          /// <returns>An IMongoCollection</returns>
          protected IMongoCollection<TDocument> GetCollection<TDocument>(string partitionKey) where TDocument : IDocument
          {
               return MongoDbContext.GetCollection<TDocument>(partitionKey);
          }

          /// <summary>
          /// Gets the collection.
          /// </summary>
          /// <typeparam name="TDocument">The type of the document.</typeparam>
          /// <returns>An IMongoCollection</returns>
          protected IMongoCollection<TDocument> GetCollection<TDocument>() where TDocument : IDocument
          {
               return MongoDbContext.GetCollection<TDocument>();
          }

          /// <summary>
          /// Handles the partitioned.
          /// </summary>
          /// <typeparam name="TDocument">The type of the document.</typeparam>
          /// <param name="document">The document.</param>
          /// <returns>An IMongoCollection</returns>
          protected IMongoCollection<TDocument> HandlePartitioned<TDocument>(TDocument document) where TDocument : IDocument
          {
               if (document is IPartitionedDocument partitionedDocument)
               {
                    return GetCollection<TDocument>(partitionedDocument.PartitionKey);
               }

               return GetCollection<TDocument>();
          }

          /// <summary>
          /// Handles the partitioned.
          /// </summary>
          /// <typeparam name="TDocument">The type of the document.</typeparam>
          /// <typeparam name="TKey">The type of the key.</typeparam>
          /// <param name="document">The document.</param>
          /// <returns>An IMongoCollection</returns>
          protected IMongoCollection<TDocument> HandlePartitioned<TDocument, TKey>(TDocument document)
              where TDocument : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               if (document is IPartitionedDocument partitionedDocument)
               {
                    return GetCollection<TDocument, TKey>(partitionedDocument.PartitionKey);
               }

               return GetCollection<TDocument, TKey>();
          }

          /// <summary>
          /// Handles the partitioned.
          /// </summary>
          /// <typeparam name="TDocument">The type of the document.</typeparam>
          /// <param name="partitionKey">The partition key.</param>
          /// <returns>An IMongoCollection</returns>
          protected IMongoCollection<TDocument> HandlePartitioned<TDocument>(string partitionKey) where TDocument : IDocument
          {
               return !string.IsNullOrEmpty(partitionKey) ? GetCollection<TDocument>(partitionKey) : GetCollection<TDocument>();
          }

          /// <summary>
          /// Gets the collection.
          /// </summary>
          /// <typeparam name="TDocument">The type of the document.</typeparam>
          /// <typeparam name="TKey">The type of the key.</typeparam>
          /// <param name="partitionKey">The partition key.</param>
          /// <returns>An IMongoCollection</returns>
          protected IMongoCollection<TDocument> GetCollection<TDocument, TKey>(string partitionKey = null)
              where TDocument : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               return MongoDbContext.GetCollection<TDocument, TKey>(partitionKey);
          }

          /// <summary>
          /// Handles the partitioned.
          /// </summary>
          /// <typeparam name="TDocument">The type of the document.</typeparam>
          /// <typeparam name="TKey">The type of the key.</typeparam>
          /// <param name="partitionKey">The partition key.</param>
          /// <returns>An IMongoCollection</returns>
          protected IMongoCollection<TDocument> HandlePartitioned<TDocument, TKey>(string partitionKey)
              where TDocument : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               return !string.IsNullOrEmpty(partitionKey) ? GetCollection<TDocument, TKey>(partitionKey) : GetCollection<TDocument, TKey>();
          }


          public virtual async Task<List<T>> GetPaginatedAsyncByReverse<T>(Expression<Func<T, bool>> filter, int skipNumber = 0, int takeNumber = 50, string partitionKey = null)
              where T : IDocument
          {
               var count = GetCollection<T>(partitionKey).CountDocuments(filter);
               skipNumber = (count == 0 || count <= takeNumber) ? 0 : (int)(count - takeNumber);
               return await HandlePartitioned<T>(partitionKey).Find(filter).Skip(skipNumber).Limit(takeNumber).ToListAsync();
          }
          #endregion

          #region DocId

          /// <summary>
          /// Sets the value of the document Id if it is not set already.
          /// </summary>
          /// <typeparam name="T">The document type.</typeparam>
          /// <param name="document">The document.</param>
          protected void FormatDocument<T>(T document) where T : IDocument
          {
               if (document == null)
               {
                    throw new ArgumentNullException(nameof(document));
               }

               if (document.Id == default(ObjectId))
               {
                    document.Id = ObjectId.GenerateNewId();
               }
          }

          /// <summary>
          /// Sets the value of the document Id if it is not set already.
          /// </summary>
          /// <typeparam name="TDocument">The document type.</typeparam>
          /// <typeparam name="TKey">The type of the primary key.</typeparam>
          /// <param name="document">The document.</param>
          protected void FormatDocument<TDocument, TKey>(TDocument document)
              where TDocument : IDocument<TKey>
              where TKey : IEquatable<TKey>
          {
               if (document == null)
               {
                    throw new ArgumentNullException(nameof(document));
               }

               var defaultTKey = default(TKey);
               if (document.Id == null
                   || (defaultTKey != null
                       && defaultTKey.Equals(document.Id)))
               {
                    document.Id = IdGenerator.GetId<TKey>();
               }
          }

          #endregion
     }
}
