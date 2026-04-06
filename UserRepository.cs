// <copyright file="UserRepository.cs" company="Wowzer">
// Copyright (c) Wowzer. All rights reserved.
// </copyright>
// <author>Clayton Fetzer</author>

using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Wowzer.Services.Models;
using Wowzer.Services.Models.Enums;
using Wowzer.Services.Mongo;
using Wowzer.Services.Repository.Interfaces;

namespace Wowzer.Services.Repository
{
     /// <summary>
     ///     The User Repository
     /// </summary>
     /// <seealso cref="BaseMongoRepository" />
     /// <seealso cref="IUserRepository" />
     public class UserRepository : BaseMongoRepository, IUserRepository
     {
          /// <inheritdoc />
          public UserRepository(string connectionString, string databaseName) : base(connectionString, databaseName)
          {
               ConnectionString = connectionString;
               DatabaseName = databaseName;
          }

          /// <inheritdoc />
          public async Task<User> AddUserAsync(User user)
          {
               var userDetail = GetOne<User>(x => x.AccountStatus != AccountStatus.Blocked 
                                                                 && 
                                                                 (
                                                                      (!string.IsNullOrEmpty(user.FacebookId) && x.FacebookId == user.FacebookId) || (!string.IsNullOrEmpty(user.Email) && x.Email == user.Email)
                                                                 )
                                                       );
               if (userDetail == null)
               {
                    await AddOneAsync(user);
                    userDetail = user;
               }

               return userDetail;
          }

          /// <inheritdoc />
          public async Task<User> GetUserAsync(string id)
          {
               return await GetOneAsync<User>(x => x.Id == ObjectId.Parse(id) && x.AccountStatus != AccountStatus.Blocked);
          }

          /// <inheritdoc />
          public async Task<User> GetUserByEmail(string emailAddress)
          {
               return await GetOneAsync<User>(x => (x.AccountStatus != AccountStatus.Blocked) && (x.Email.ToLower() == emailAddress.ToLower()));
          }

          public async Task<User> GetUserByFacebookId(string fbId)
          {
               return await GetOneAsync<User>(x => (x.AccountStatus != AccountStatus.Blocked) && (x.FacebookId.ToLower() == fbId.ToLower()));
          }

          /// <inheritdoc />
          public async Task<bool> UpdateUserAsync(User user)
          {
               return await UpdateOneAsync(user);
          }

          /// <inheritdoc />
          public async Task<IEnumerable<User>> GetUserProfileDeckAsync(string profileId, int skip, int limit)
          {
               ////TODO: Need to refine the logic when performance issue occured
               //// Get the current user details
               var currentUser = await GetOneAsync<User>(user => user.Id.Equals(new ObjectId(profileId)) && (user.AccountStatus != AccountStatus.Blocked) && (user.IsPublished));
               if (currentUser == null)
               {
                    return null;
               }

               var settings = GetOne<SearchSettings>(x => x.ProfileId.Equals(currentUser.Id));
               if (settings == null)
               {
                    settings = new SearchSettings()
                    {
                         ProfileId = currentUser.Id,
                         AgeRange = new List<int> { 18, 80 },
                         Distance = 100
                    };
                    await AddOneAsync(settings);
               }

               /*TODO move this logic to business layer.*/
               var seekingFor = string.Empty;
               seekingFor = (currentUser.Seeking != null && currentUser.Seeking.Count > 0) ?
                                                                                          (currentUser.Seeking.Contains(Gender.EveryOne) ?
                                                                                                                                       (string.Join(',', "0", "1", "2", "3")) :
                                                                                                                                       (string.Join(',', currentUser.Seeking.Select(x => (int)x))))
                                                                                          :
                                                                                          (string.Join(',', "0", "1"));
               var upperBound = settings.AgeRangeUpperBound < 80 ? DateTime.Today.AddYears(-settings.AgeRangeUpperBound).Date.ToString("yyyy-MM-ddTHH:mm:ss") : string.Empty;

               //Parse json query into document object
               var query = BsonDocument.Parse("{" +
                                               "Location:{" +
                                                   "$nearSphere:{" +
                                                       "$geometry: {" + /*Co-ordinates Criteria*/
                                                           "type:'Point'," +
                                                           "coordinates:[" + currentUser.Location[0] + "," + currentUser.Location[1] +
                                                           "]}," +
                                                           "$minDistance: " + (0 * 1609.34) + "," + /*Will get Distance in Miles. Converting miles into meters */
                                                           "$maxDistance: " + (settings.Distance * 1609.34) + /*$geometry search will supports in meters.*/
                                                           "}" +
                                                       "}," +
                                                   "IsPublished:true," + /*Is Published*/
                                                                         //    "BirthDate:{" + /*Age criteria*/
                                                                         //   upperBound +
                                                                         //    "$lte:\"" + DateTime.Today.AddYears(-settings.AgeRangeLowerBound).Date.ToString("yyyy-MM-ddTHH:mm:ss") + "\"" +
                                                                         //"}" +
                                              "Gender:{$in:[" + seekingFor + "]}," + /*Male*/
                                              "AccountStatus:{$ne:" + (int)AccountStatus.Blocked + "}," + //Excluding blocked profiles
                                              "SeekingFor:{$in:[" + (int)currentUser.Gender + "]}" +
                                           "}");


               //// Find result based on search criteria in Users collection

               var result = MongoDbContext.Database.GetCollection<User>("Profile").Find(query);
               var searchResult = result != null ? result.ToList() : new List<User>();
               if (searchResult != null && searchResult.Count() > 0)
               {
                    var filterProfileMatches = GetAll<ProfileMatch>(x => (x.ProfileId == currentUser.Id) || (x.MatchedProfileId == currentUser.Id));


                    //var disLikeProfiles = filterProfileMatches.Where(x =>
                    //                              (x.ProfileId == settings.ProfileId && (x.ProfileIdStatus.HasFlag(MatchStatus.DisLike)))
                    //                              ||
                    //                              (x.MatchedProfileId == settings.ProfileId
                    //                                        &&
                    //                                        x.MatchedProfileIdStatus.HasFlag(MatchStatus.DisLike))
                    //                              ).OrderBy(pm => (pm.ProfileId == settings.ProfileId) ? pm.ProfileLastSwipedDate : pm.MatchedProfileLastSwipedDate).ToList();

                    var profileMatches = filterProfileMatches.Where(x =>
                                                  (x.ProfileId == settings.ProfileId && (x.ProfileIdStatus.HasFlag(MatchStatus.Like) || x.ProfileIdStatus.HasFlag(MatchStatus.DisLike) || x.ProfileIdStatus.HasFlag(MatchStatus.ActiveChat) || x.MatchStatus.HasFlag(MatchStatus.Blocked) || x.MatchStatus.HasFlag(MatchStatus.Unmatched)))
                                                  ||
                                                  (x.MatchedProfileId == settings.ProfileId
                                                            &&
                                                            (x.MatchedProfileIdStatus.HasFlag(MatchStatus.Like) || x.MatchedProfileIdStatus.HasFlag(MatchStatus.DisLike) || x.MatchedProfileIdStatus.HasFlag(MatchStatus.ActiveChat) || x.MatchStatus.HasFlag(MatchStatus.Blocked) || (x.MatchStatus.HasFlag(MatchStatus.Unmatched))))
                                                  ).OrderByDescending(pm => pm.MatchStatus);
                    //  Age criteria
                    searchResult = upperBound != string.Empty ?
                        searchResult?.Where(u => (u.Id != settings.ProfileId)
                                                                &&
                                                                (
                                                                ((new DateTime(DateTime.Now.Subtract(u.BirthDate).Ticks).Year - 1) >= (new DateTime(DateTime.Now.Subtract(DateTime.Today.AddYears(-settings.AgeRangeLowerBound)).Ticks).Year - 1))
                                                                          &&
                                                               ((new DateTime(DateTime.Now.Subtract(u.BirthDate).Ticks).Year - 1) <= (new DateTime(DateTime.Now.Subtract(DateTime.Today.AddYears(-settings.AgeRangeUpperBound)).Ticks).Year - 1))

                                                                )
                                                 ).ToList()
                        : searchResult?.Where(u => (u.Id != settings.ProfileId)
                                                                 &&
                                                                 (
                                                                     (new DateTime(DateTime.Now.Subtract(u.BirthDate).Ticks).Year - 1) >= (new DateTime(DateTime.Now.Subtract(DateTime.Today.AddYears(-settings.AgeRangeLowerBound)).Ticks).Year - 1)
                                                                 )
                                                  ).ToList();

                    // exclude profile match results from search result
                    var matchResults = new List<User>();
                    List<User> finalList = new List<User>();
                    List<User> disLikeUsers = new List<User>();
                    if (profileMatches.ToList().Count > 0)
                    {
                         matchResults = searchResult.Where(u => profileMatches.Any(pMatch => pMatch.ProfileId == u.Id || pMatch.MatchedProfileId == u.Id) == false).ToList();
                         //finalList = matchResults.Count() > 0 ? matchResults.ToList() : (matchResults.Concat(searchResult.Where(u => disLikeProfiles.Any(pMatch => pMatch.ProfileId == u.Id || pMatch.MatchedProfileId == u.Id) == true))).ToList();
                         //if (limit > matchResults.Count())
                         //{
                         //     for (int pmIndex = 0; pmIndex < disLikeProfiles.Count(); pmIndex++)
                         //     {
                         //          for (int pIndex = 0; pIndex < searchResult.Count(); pIndex++)
                         //          {
                         //               if (disLikeProfiles[pmIndex].ProfileId == searchResult[pIndex].Id || disLikeProfiles[pmIndex].MatchedProfileId == searchResult[pIndex].Id)
                         //               {
                         //                    disLikeUsers.Add(searchResult[pIndex]);
                         //                    break;
                         //               }
                         //          }
                         //     }
                         //     finalList = matchResults.Concat(disLikeUsers).ToList();
                         //     limit = (limit == 0 || limit >= finalList.Count()) ? finalList.Count() : limit;
                         //     finalList = finalList.Take(limit).ToList();
                         //}

                         if (matchResults.Count() > 0)
                         {
                              limit = (limit == 0 || limit >= matchResults.Count()) ? matchResults.Count() : limit;
                              finalList = matchResults.Take(limit).ToList();
                         }
                         //else if (matchResults.Count() == 0)
                         //{
                         //     limit = (limit == 0 || limit >= searchResult.Count()) ? searchResult.Count() : limit;
                         //     finalList = searchResult.Take(limit).ToList();
                         //}

                    }
                    else if (searchResult.Count() > 0)
                    {
                         limit = (limit == 0 || limit >= searchResult.Count()) ? searchResult.Count() : limit;
                         finalList = searchResult.Take(limit).ToList();
                    }

                    return finalList;
               }

               return new List<User>();
          }

          /// <inheritdoc />
          public async Task<bool> UpdateSearchSettingsAsync(SearchSettings settings)
          {
               var update = Builders<SearchSettings>.Update
                   .Set(x => x.AgeRange, settings.AgeRange)
                   .Set(x => x.Distance, settings.Distance);

               var filter = Builders<SearchSettings>.Filter
                   .Where(x => x.ProfileId == settings.ProfileId);

               return await UpdateOneAsync(filter, update);
          }

          /// <inheritdoc />
          public async Task<SearchSettings> GetSearchSettingsAsync(string userId)
          {
               var searchSettings = GetOne<SearchSettings>(x => x.ProfileId.Equals(new ObjectId(userId)));
               if (searchSettings == null)
               {
                    var settings = new SearchSettings()
                    {
                         ProfileId = ObjectId.Parse(userId),
                         AgeRange = new List<int> { 18, 80 },
                         Distance = 100
                    };

                    await AddOneAsync(settings);
                    searchSettings = settings;
               }
               return searchSettings;
          }

          /// <inheritdoc />
          public async Task DisableUserAsync(DeletedAccount account)
          {
               await Task.WhenAll(
                   AddOneAsync(account),
                 UpdateOneAsync<User, AccountStatus>(x => x.Id.Equals(account.ProfileId), user => user.AccountStatus, AccountStatus.Blocked));
          }

          /// <inheritdoc />
          public async Task InsertContactUs(ContactUs contact)
          {
               await AddOneAsync(contact);
          }

          /// <inheritdoc />
          public async Task<IList<User>> GetMatchProfilesAsync(string profileId, int skip, int limit)
          {
               //var filterProfileMatches = await GetAllAsync<ProfileMatch>(
               //                                                                x => (
               //                                                                           (
               //                                                                                 (x.ProfileId == ObjectId.Parse(profileId)) || (x.MatchedProfileId == ObjectId.Parse(profileId))
               //                                                                           )
               //                                                                                &&
               //                                                                          (
               //                                                                                (x.ProfileIdStatus.HasFlag(MatchStatus.Like) && (x.MatchedProfileIdStatus.HasFlag(MatchStatus.Like)))
               //                                                                                  &&
               //                                                                                x.MatchStatus.HasFlag(MatchStatus.Like) || x.MatchStatus.HasFlag(MatchStatus.ActiveChat)
               //                                                                          )
               //                                                                     )
               //                                                           );

               var filterProfileMatches = await GetPaginatedAsync<ProfileMatch>(
                                                                                     (
                                                                                           x => (
                                                                                                    (
                                                                                                         (x.ProfileId == ObjectId.Parse(profileId)) || (x.MatchedProfileId == ObjectId.Parse(profileId))
                                                                                                    )
                                                                                                    &&
                                                                                                    (
                                                                                                          (x.ProfileIdStatus.HasFlag(MatchStatus.Like) && (x.MatchedProfileIdStatus.HasFlag(MatchStatus.Like)))
                                                                                                               &&
                                                                                                          x.MatchStatus.HasFlag(MatchStatus.Like) || x.MatchStatus.HasFlag(MatchStatus.ActiveChat)
                                                                                                     )

                                                                                                  )
                                                                                     ),
                                                                                    skip,
                                                                                    limit
                                                                                );

               if (filterProfileMatches == null)
               {
                    return null;
               }

               if (filterProfileMatches.Count() > 0)
               {
                    filterProfileMatches = filterProfileMatches.OrderBy(pm => pm.MatchedDate).ToList();
                    var profileIds = filterProfileMatches.Select(x => x.ProfileId == ObjectId.Parse(profileId) ? x.MatchedProfileId : x.ProfileId).OrderBy(x => x.Pid).ToList();
                    var filter = Builders<User>.Filter.In(p => p.Id, profileIds);
                    var profiles = MongoDbContext.Database.GetCollection<User>("Profile").Find(filter);
                    var temp = profiles.ToList();
                    temp = temp.Where(x => x.IsPublished && x.AccountStatus != AccountStatus.Blocked).ToList();
                    for (var i = 0; i < temp.Count(); i++)
                    {
                         temp[i].IsChatInitiated = filterProfileMatches.Where(pr => pr.ProfileId == temp[i].Id || pr.MatchedProfileId == temp[i].Id).FirstOrDefault().IsChatInitiated;
                    }
                    return temp;
               }

               return null;
          }

          /// <inheritdoc />
          public async Task<(IList<User> activeChatList, int unReadCount)> GetActiveChatProfilesAsync(string profileId, int skip, int limit)
          {

               var count = await GetUnreadMessagesCount(profileId);
               var filterProfileMatches = await GetPaginatedAsync<ProfileMatch>(
                                                                                  (x =>
                                                                                   (
                                                                                        (x.MatchStatus == MatchStatus.ActiveChat)
                                                                                        &&
                                                                                         (
                                                                                              (x.ProfileId == ObjectId.Parse(profileId) && x.IsProfileChatActive && !x.IsProfileChatHide)
                                                                                                   ||
                                                                                              (x.MatchedProfileId == ObjectId.Parse(profileId) && x.IsMatchedProfileChatActive && !x.IsMatchedProfileChatHide)
                                                                                          )
                                                                                   )
                                                                                   )
                                                                                   , skip
                                                                                   , limit
                                                                          );

               if (filterProfileMatches == null)
               {
                    return (null, count);
               }

               if (filterProfileMatches.Count() > 0)
               {
                    var profileIds = filterProfileMatches.Select(x => x.ProfileId == ObjectId.Parse(profileId) ? x.MatchedProfileId : x.ProfileId).ToList();
                    var filter = Builders<User>.Filter.In(p => p.Id, profileIds);
                    var profiles = MongoDbContext.Database.GetCollection<User>("Profile").Find(filter).ToList();

                    for (var i = 0; i < profiles.Count; i++)
                    {
                         var message = GetAll<Message>(
                                                              x =>
                                                             (
                                                                  (
                                                                       (x.ProfileId == ObjectId.Parse(profileId)) && (x.RecipientProfileId == profiles[i].Id)
                                                                  )
                                                                            ||
                                                                  (
                                                                       (x.ProfileId == profiles[i].Id) && (x.RecipientProfileId == ObjectId.Parse(profileId))
                                                                  )
                                                              )

                                                        ).OrderByDescending(x => x.SentTime).FirstOrDefault();
                         profiles[i].Chat = message;
                         profiles[i].IsChatInitiated = true;
                    }
                    var sortedProfiles = profiles.Where(x => x.IsPublished && x.AccountStatus != AccountStatus.Blocked).OrderByDescending(pr => pr.Chat != null ? pr.Chat.SentTime : new DateTime()).ToList();

                    return (sortedProfiles, count);
               }

               return (null, count);
          }

          //public IList<UserImage> GetUserImages(IList<UserImage> userImages)
          //{
          //     foreach (var image in userImages)
          //     {
          //          image.Image = image.Image.Replace("profileimages", "thumbnails");
          //     }

          //     return userImages;
          //}

          /// <inheritdoc />
          public async Task<bool> DeleteUserAsync(string userId)
          {
               User user = new User();
               user.Id = MongoDB.Bson.ObjectId.Parse(userId);
               return await UpdateOneAsync<User, bool>(user, u => u.IsPublished, false);
          }

          /// <inheritdoc />
          public async Task<(bool isBlocked, int unReadCount)> BlockOrReportUserAsync(BlockedUser blockedUser)
          {
               await AddOneAsync(blockedUser);

               var profile = GetOne<ProfileMatch>(x =>
                                                           (
                                                                (
                                                                     (x.ProfileId == ObjectId.Parse(blockedUser.BlockedByProfileId)) && (x.MatchedProfileId == ObjectId.Parse(blockedUser.ProfileId))
                                                                )
                                                                          ||
                                                                (
                                                                     (x.ProfileId == ObjectId.Parse(blockedUser.ProfileId)) && (x.MatchedProfileId == ObjectId.Parse(blockedUser.BlockedByProfileId))
                                                                )
                                                            )
                                                     );

               var update = (profile.ProfileId == MongoDB.Bson.ObjectId.Parse(blockedUser.BlockedByProfileId)) && (profile.MatchedProfileId == MongoDB.Bson.ObjectId.Parse(blockedUser.ProfileId)) ?
                                                                                                         Builders<ProfileMatch>.Update
                                                                                                         .Set(x => x.MatchStatus, MatchStatus.Blocked)
                                                                                                         .Set(x => x.ProfileIdStatus, MatchStatus.Blocked)
                                                                                                              :
                                                                                                         Builders<ProfileMatch>.Update
                                                                                                         .Set(x => x.MatchStatus, MatchStatus.Blocked)
                                                                                                         .Set(x => x.MatchedProfileIdStatus, MatchStatus.Blocked);
               //var update = Builders<ProfileMatch>.Update
               //    .Set(x => x.MatchStatus, MatchStatus.Blocked);

               //var filter = Builders<ProfileMatch>.Filter
               //    .Where(x => x.ProfileId == MongoDB.Bson.ObjectId.Parse(blockedUser.ProfileId) && x.MatchedProfileId == MongoDB.Bson.ObjectId.Parse(blockedUser.BlockedByProfileId));

               var messages = GetAll<Message>(m => (!m.IsRead && m.IsMatchedProfileMessageActive)
                                                       &&
                                                       (
                                                            (m.RecipientProfileId == new ObjectId(blockedUser.BlockedByProfileId) && m.ProfileId == new ObjectId(blockedUser.ProfileId))
                                                                 ||
                                                            (m.ProfileId == new ObjectId(blockedUser.BlockedByProfileId) && m.RecipientProfileId == new ObjectId(blockedUser.ProfileId))
                                                       )
                                                   );

               await UpdateManyAsync<Message, bool>(messages, m => m.IsRead, true);

               var count = GetUnreadMessagesCount(blockedUser.BlockedByProfileId).Result;

               bool isBlocked = await UpdateOneAsync(profile, update);
               return (isBlocked, count);
          }

          public async Task<bool> SignUp(User signUp)
          {
               //var user = string.IsNullOrWhiteSpace(signUp.Email) ? null : GetOne<User>(u => u.Email.Trim() == signUp.Email.Trim());
               //if (user != null)
               //{
               //     return true;
               //}
               //await AddOneAsync(signUp);
               return false;
          }

          public async Task<bool> SignIn(User signUp)
          {
               //     var user = string.IsNullOrWhiteSpace(signUp.Email) ? null : GetOne<User>(u => u.Email.Trim() == signUp.Email.Trim() && (!string.IsNullOrEmpty(u.Password)));
               //     if (user != null)
               //     {
               //          return true ? (user.Email.Trim() == signUp.Email.Trim() && user.Password.Trim() == signUp.Password.Trim()) : false;
               //     }
               return false;
          }

          public async Task<bool> ResetPassword(User signUp)
          {
               //var user = string.IsNullOrWhiteSpace(signUp.Email) ? null : GetOne<User>(u => u.Email.Trim() == signUp.Email.Trim());
               //if (user != null)
               //{
               //     return await UpdateOneAsync<User, string>(user, u => u.Password, signUp.Password);
               //}
               return false;
          }

          public async Task<ProfileMatch> GetProfileMatch(string profileId, string matchProfileId)
          {
               return await GetOneAsync<ProfileMatch>(pm => (pm.MatchStatus == MatchStatus.ActiveChat || pm.MatchStatus == MatchStatus.Like)
                                                                   &&
                                                                   ((pm.ProfileId == new ObjectId(profileId) && pm.MatchedProfileId == new ObjectId(matchProfileId))
                                                                           ||
                                                                       (pm.ProfileId == new ObjectId(matchProfileId) && pm.MatchedProfileId == new ObjectId(profileId))
                                                                   )
                                                       );
          }

          private async Task<int> GetUnreadMessagesCount(string profileId)
          {
               int pCount = 0;

               var profileMessages = await GetAllAsync<Message>(msg => (!msg.IsRead && msg.IsMatchedProfileMessageActive) && (msg.RecipientProfileId == ObjectId.Parse(profileId))) as IEnumerable<Message>;

               var profileIds = profileMessages.Select(x => x.ProfileId == ObjectId.Parse(profileId) ? x.RecipientProfileId : x.ProfileId).OrderBy(x => x.Pid).Distinct().ToList();
               var filter = Builders<User>.Filter.In(p => p.Id, profileIds);
               var profiles = MongoDbContext.Database.GetCollection<User>("Profile").Find(filter).ToList();
               var messages = profileMessages != null ? profileMessages.Where(msg => profiles.Any(u => u.AccountStatus == AccountStatus.Active)).ToList() : new List<Message>();
               pCount = messages != null ? messages.Count() : 0;

               return pCount;
          }

          public async Task<User> GetUserByEmailIdOrFacebookId(string facebookId, string emailId)
          {
               return await GetOneAsync<User>(x => (x.AccountStatus != AccountStatus.Blocked)
                                              &&
                                        (
                                             ((!string.IsNullOrEmpty(facebookId)) && x.FacebookId == facebookId)
                                                  ||
                                             (!string.IsNullOrEmpty(emailId) && x.Email == emailId)
                                         )
                                   );
          }
     }
}
