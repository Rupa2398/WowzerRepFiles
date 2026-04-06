// <copyright file="SearchUserActionsRepository.cs" company="Wowzer">
// Copyright (c) Wowzer. All rights reserved.
// </copyright>
// <author>Lakshmi narayana G</author>
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Linq;
using System.Threading.Tasks;
using Wowzer.Services.Models;
using Wowzer.Services.Models.DTO;
using Wowzer.Services.Models.Enums;
using Wowzer.Services.Mongo;
using Wowzer.Services.Repository.Interfaces;

namespace Wowzer.Services.Repository
{
     /// <summary>
     /// Repository for Search user actions
     /// </summary>
     public class SearchUserActionsRepository : BaseMongoRepository, ISearchUserActionsRepository
     {
          /// <summary>
          /// Initializes a new instance of the <see cref="SearchUserActionsRepository" /> class.
          /// </summary>
          /// <param name="connectionString">MongoDB connection string</param>
          /// <param name="databaseName">MongoDB database name</param>
          public SearchUserActionsRepository(string connectionString, string databaseName) : base(connectionString, databaseName)
          {
               ConnectionString = connectionString;
               DatabaseName = databaseName;
          }

          /// <summary>
          /// <Inheritdoc />
          /// </summary>
          /// <param name="profileMatch">ProfileMatch DTO</param>
          /// <returns>Task result</returns>
          public async Task<(MatchStatus status, int unReadCount)> UpdateSearchActionsAsync(ProfileMatchDTO profileMatch)
          {
               ////Check if the user is already request by MatchedProfileId
               //var matchProfile = await GetOneAsync<ProfileMatch>(p =>
               //    p.ProfileId.Equals(ObjectId.Parse(profileMatch.MatchedProfileId)) &&
               //     p.MatchedProfileId.Equals(ObjectId.Parse(profileMatch.ProfileId)));

               var matchStatus = new MatchStatus();
               var matchProfile = await GetOneAsync<ProfileMatch>(x =>
                                                          (
                                                               (
                                                                    (x.ProfileId == ObjectId.Parse(profileMatch.ProfileId)) && (x.MatchedProfileId == ObjectId.Parse(profileMatch.MatchedProfileId))
                                                               )
                                                                         ||
                                                               (
                                                                    (x.ProfileId == ObjectId.Parse(profileMatch.MatchedProfileId)) && (x.MatchedProfileId == ObjectId.Parse(profileMatch.ProfileId))
                                                               )
                                                           )
                                                    );

               ////MatchedProfileId exists, update MatchedStatus with current action
               if (matchProfile != null)
               {
                    matchProfile.MatchStatus = profileMatch.SwipeAction;
                    if ((matchProfile.ProfileId == ObjectId.Parse(profileMatch.ProfileId)) && (matchProfile.MatchedProfileId == ObjectId.Parse(profileMatch.MatchedProfileId)))
                    {
                         matchProfile.ProfileIdStatus = profileMatch.SwipeAction;
                         matchStatus = matchProfile.MatchedProfileIdStatus;
                         matchProfile.ProfileLastSwipedDate = DateTime.UtcNow;
                         matchProfile.MatchedDate = profileMatch.SwipeAction == MatchStatus.Like && (matchProfile.MatchedProfileIdStatus == MatchStatus.Like) ? DateTime.UtcNow : matchProfile.MatchedDate;
                    }
                    else
                    {
                         matchProfile.MatchedProfileIdStatus = profileMatch.SwipeAction;
                         matchStatus = matchProfile.ProfileIdStatus;
                         matchProfile.MatchedProfileLastSwipedDate = DateTime.UtcNow;
                         matchProfile.MatchedDate = profileMatch.SwipeAction == MatchStatus.Like && (matchProfile.ProfileIdStatus == MatchStatus.Like) ? DateTime.UtcNow : matchProfile.MatchedDate;
                    }


                    ////Set fields for Insert/Update
                    var update = Builders<ProfileMatch>.Update
                         .Set(a => a.ProfileLastSwipedDate, matchProfile.ProfileLastSwipedDate)
                        .Set(a => a.MatchedProfileLastSwipedDate, matchProfile.MatchedProfileLastSwipedDate)
                        .Set(a => a.ProfileId, matchProfile.ProfileId)
                        .Set(a => a.MatchedProfileId, matchProfile.MatchedProfileId)
                        .Set(a => a.MatchStatus, matchProfile.MatchStatus)
                        .Set(a => a.ProfileIdStatus, matchProfile.ProfileIdStatus)
                        .Set(a => a.MatchedProfileIdStatus, matchProfile.MatchedProfileIdStatus)
                        .Set(a => a.MatchedDate, matchProfile.MatchStatus == MatchStatus.Like ? DateTime.UtcNow : matchProfile.MatchedDate)
                        .Set(a => a.DateAddedUtc, matchProfile.DateAddedUtc)
                        .Set(a => a.MatchStatus, matchProfile.MatchStatus)
                        .Set(a => a.IsChatInitiated, matchProfile.IsChatInitiated)
                        .Set(a => a.IsMatchedProfileChatActive, true)
                        .Set(a => a.IsProfileChatActive, true)
                        .Set(a => a.IsProfileChatHide, false)
                        .Set(a => a.IsMatchedProfileChatHide, false)
                        ;
                    //// Updte matched profiles
                    await UpdateOneAsync<ProfileMatch>(matchProfile, update);
                    if (profileMatch.SwipeAction == MatchStatus.Unmatched)
                    {
                         var messages = GetAll<Message>(m => (!m.IsRead)
                                                                 &&
                                                                 (
                                                                     (m.RecipientProfileId == new MongoDB.Bson.ObjectId(profileMatch.ProfileId) && m.ProfileId == new ObjectId(profileMatch.MatchedProfileId))
                                                                     ||
                                                                     (m.RecipientProfileId == new MongoDB.Bson.ObjectId(profileMatch.MatchedProfileId) && m.ProfileId == new ObjectId(profileMatch.ProfileId))
                                                                 )
                                                        );

                         await UpdateManyAsync<Message, bool>(messages, m => m.IsRead, true);
                    }

               }
               else
               {
                    matchProfile = new ProfileMatch()
                    {
                         ProfileId = ObjectId.Parse(profileMatch.ProfileId),
                         MatchedProfileId = ObjectId.Parse(profileMatch.MatchedProfileId),
                         ProfileIdStatus = profileMatch.SwipeAction,
                         MatchedProfileIdStatus = MatchStatus.None,
                         MatchedDate = DateTime.UtcNow,
                         MatchStatus = profileMatch.SwipeAction,
                         IsChatInitiated = false,
                         IsMatchedProfileChatActive = true,
                         IsProfileChatActive = true,
                         IsProfileChatHide = false,
                         IsMatchedProfileChatHide = false,
                         ProfileLastSwipedDate = DateTime.UtcNow
                    };

                    ////Insert  matched profiles

                    await AddOneAsync<ProfileMatch>(matchProfile);

               }
               var count = GetAll<Message>(m => (!m.IsRead && m.IsProfileMessageActive)
                                                       &&
                                                       (m.RecipientProfileId == new ObjectId(profileMatch.ProfileId))
                                                   ).Count();

               return (matchStatus, count);
          }
     }
}
