// <copyright file="MasterRepository.cs" company="Wowzer">
// Copyright (c) Wowzer. All rights reserved.
// </copyright>
// <author>Shiva Kumar </author>

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Wowzer.Services.Models;
using Wowzer.Services.Models.Master;
using Wowzer.Services.Models.Mongo;
using Wowzer.Services.Mongo;
using Wowzer.Services.Repository.Interfaces;

namespace Wowzer.Services.Repository
{
     /// <summary>
     ///     The Master Repository
     /// </summary>
     /// <seealso cref="BaseMongoRepository" />
     /// <seealso cref="IMasterRepository" />
     public class MasterRepository : BaseMongoRepository, IMasterRepository
     {
          /// <inheritdoc />
          public MasterRepository(string connectionString, string databaseName) : base(connectionString, databaseName)
          {
               ConnectionString = connectionString;
               DatabaseName = databaseName;
          }

          /// <inheritdoc />
          public IList<TDocument> GetMasterDetails<TDocument>() where TDocument : IDocument
          {
               try
               {
                    return GetAll<TDocument>(_ => true);
               }

               catch (Exception ex)
               {
                    throw;
               }
          }

          public async Task ExportCollection(string document, dynamic list)
          {
               var connectionString = "mongodb://test:test1234@ds040027.mlab.com:40027/wowzeruat?retryWrites=false";
               var dbName = "wowzeruat";
               var context = new MongoDbContext(connectionString, dbName);
               var db = context.Client.GetDatabase(dbName);

               //var areasons = GetAll<AccountDeletedReason>(a => a.Reason != null);
               //db.GetCollection<AccountDeletedReason>(document).InsertMany(areasons);

               //var locations = GetAll<Locations>(l => l.Location.Length > 0);
               //list = locations;
               //var dogBreed = GetAll<DogBreed>(p => p.Breed != null);
               //dogBreed = dogBreed.OrderBy(b => b.Breed).ToList();

               //for (var index = 0; index < dogBreed.Count; index++)
               //{
               //     var breed = new DogBreed() {
               //          BreedId = index+1,
               //          Breed = dogBreed[index].Breed
               //     };
               //     db.GetCollection<DogBreed>(document).InsertOne(breed);
               //}

               //var profile = GetAll<User>(u => u.Name != null);
               //db.GetCollection<User>(document).InsertMany(profile);

               //var professions = GetAll<Profession>(p => p.Name != "Other");

               //for (var index = 0; index < professions.Count; index++)
               //{
               //     professions[index].ProfessionId = index + 1;
               //     db.GetCollection<Profession>(document).InsertOne(professions[index]);
               //}
               //var profess = new Profession();
               //profess.Name = "Other";
               //profess.ProfessionId = professions.Count + 1;
               //db.GetCollection<Profession>(document).InsertOne(profess);

               await Task.Run(() =>
               {
                    switch ((ImportExportMaster)Enum.Parse(typeof(ImportExportMaster), document))
                    {
                         case ImportExportMaster.Education:
                              db.GetCollection<Education>(document).InsertMany(list);
                              break;
                         case ImportExportMaster.Profession:
                              db.GetCollection<Profession>(document).InsertMany(list);
                              break;
                         case ImportExportMaster.DogBreed:
                              db.GetCollection<DogBreed>(document).InsertMany(list);
                              break;
                         case ImportExportMaster.DogSize:
                              db.GetCollection<DogSize>(document).InsertMany(list);
                              break;
                         case ImportExportMaster.AccountDeletedReason:
                              db.GetCollection<AccountDeletedReason>(document).InsertMany(list);
                              break;
                         case ImportExportMaster.Locations:
                              db.GetCollection<Locations>(document).InsertMany(list);
                              break;
                         case ImportExportMaster.UserLocations:
                              db.GetCollection<UserLocations>(document).InsertMany(list);
                              break;
                         case ImportExportMaster.Profile:
                              db.GetCollection<User>(document).InsertMany(list);
                              break;
                         case ImportExportMaster.SearchSettings:
                              db.GetCollection<SearchSettings>(document).InsertMany(list);
                              break;
                         case ImportExportMaster.ProfileMatches:
                              db.GetCollection<ProfileMatch>(document).InsertMany(list);
                              break;
                    }
               });
          }

          public async Task<ActiveUserCount> GetActiveProfileCount()
          {
               var activeUserCount = new ActiveUserCount();
               try
               {
                    var totalRegisteredUsers = await GetAllAsync<User>(u => u.Id != null);
                    var totalActiveUsers = await GetAllAsync<User>(u => u.AccountStatus == 0);
                    var totalActiveMaleUsers = await GetAllAsync<User>(u => u.AccountStatus == 0 && u.Gender == Models.Enums.Gender.Man);
                    var totalActiveFemaleUsers = await GetAllAsync<User>(u => u.AccountStatus == 0 && u.Gender == Models.Enums.Gender.Woman);
                    var totalActiveGenderNonConformingUsers = await GetAllAsync<User>(u => u.AccountStatus == 0 && u.Gender == Models.Enums.Gender.GenderNonconforming);
                    var totalDogProfiles = await GetAllAsync<User>(u => u.AccountStatus == 0 && u.Dogs != null && u.Dogs.Count > 0);
                    var totalRightSwipes = await GetAllAsync<ProfileMatch>(pm => pm.MatchedProfileIdStatus == Models.Enums.MatchStatus.Like || pm.ProfileIdStatus == Models.Enums.MatchStatus.Like);
                    var rightSwipesByMen = new List<ProfileMatch>();
                    var rightSwipesByWomen = new List<ProfileMatch>();
                    var rightSwipesByGenderNonConfirming = new List<ProfileMatch>();

                    foreach (var user in totalActiveUsers)
                    {
                         var x = totalRightSwipes.Where(rs => (user.Gender == Models.Enums.Gender.Man) && (rs.ProfileId == user.Id && rs.ProfileIdStatus == Models.Enums.MatchStatus.Like) || (rs.MatchedProfileId == user.Id && rs.MatchedProfileIdStatus == Models.Enums.MatchStatus.Like)).ToList();
                         rightSwipesByMen.AddRange(x);
                         var y = totalRightSwipes.Where(rs => (user.Gender == Models.Enums.Gender.Woman) && (rs.ProfileId == user.Id && rs.ProfileIdStatus == Models.Enums.MatchStatus.Like) || (rs.MatchedProfileId == user.Id && rs.MatchedProfileIdStatus == Models.Enums.MatchStatus.Like)).ToList();
                         rightSwipesByWomen.AddRange(y);
                         var z = totalRightSwipes.Where(rs => (user.Gender == Models.Enums.Gender.GenderNonconforming) && (rs.ProfileId == user.Id && rs.ProfileIdStatus == Models.Enums.MatchStatus.Like) || (rs.MatchedProfileId == user.Id && rs.MatchedProfileIdStatus == Models.Enums.MatchStatus.Like)).ToList();
                         rightSwipesByGenderNonConfirming.AddRange(z);
                    }

                    activeUserCount.TotalRegisteredAccounts = totalRegisteredUsers.Count;
                    activeUserCount.TotalActiveAccounts = totalActiveUsers.Count;
                    activeUserCount.TotalActiveMaleAccounts = totalActiveMaleUsers.Count;
                    activeUserCount.TotalActiveFemaleAccounts = totalActiveFemaleUsers.Count;
                    activeUserCount.TotalActiveGenderNonConformingAccounts = totalActiveGenderNonConformingUsers.Count;
                    activeUserCount.TotalDogProfiles = totalDogProfiles.Count;
                    activeUserCount.TotalRightSwipes = totalRightSwipes.Count;
                    activeUserCount.TotalRightSwipesByMen = rightSwipesByMen.Count;
                    activeUserCount.TotalRightSwipesByWomen = rightSwipesByWomen.Count;
                    activeUserCount.TotalRightSwipesByGenderNonGenderNonConforming = rightSwipesByGenderNonConfirming.Count;
                    var stateWiserUsers = totalActiveUsers.GroupBy(u => u.State, (key, values) => new StateWiseUsersCount
                    {
                         State = key,
                         Total = values.ToList().Count
                    }).OrderBy(u => u.State).ToList();
                    var ageWiserUsers = totalActiveUsers.GroupBy(u => u.Age, (key, values) => new AgeWiseUsersCount
                    {
                         Age = key,
                         Total = values.ToList().Count
                    }).OrderBy(u => u.Age).ToList();
                    activeUserCount.AgeWiseUsersCount = ageWiserUsers;
                    activeUserCount.StateWiseUsersCount = stateWiserUsers;
               }
               catch (Exception ex)
               {
                    throw;
               }
               return activeUserCount;
          }
     }
}
