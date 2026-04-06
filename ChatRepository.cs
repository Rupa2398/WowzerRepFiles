using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Wowzer.Services.Models;
using Wowzer.Services.Models.DTO;
using Wowzer.Services.Models.Enums;
using Wowzer.Services.Mongo;
using Wowzer.Services.Repository.Interfaces;

namespace Wowzer.Services.Repository
{
     public class ChatRepository : BaseMongoRepository, IChatRepository
     {
          public ChatRepository(string connectionString, string databaseName) : base(connectionString, databaseName)
          {
               ConnectionString = connectionString;
               DatabaseName = databaseName;
          }
          public async Task InsertMassage(MessageDTO message, bool isChatInitiated)
          {
               message.SentTime = DateTime.UtcNow.ToString();
               message.ReceivedTime = DateTime.UtcNow.ToString();
               message.IsActive = true;

               var matchProfile = GetOne<ProfileMatch>(p =>
                                                            (p.MatchStatus == MatchStatus.ActiveChat || p.MatchStatus == MatchStatus.Like)
                                                                 &&
                                                                 ((p.ProfileId == new ObjectId(message.ProfileId) && p.MatchedProfileId == new ObjectId(message.RecipientProfileId))
                                                                       ||
                                                                      (p.ProfileId == new ObjectId(message.RecipientProfileId) && p.MatchedProfileId == new ObjectId(message.ProfileId))
                                                                  )
                                                  );
               if (matchProfile != null)
               {

                    var update = matchProfile.ProfileId == new ObjectId(message.ProfileId) ? Builders<ProfileMatch>.Update
                      .Set(a => a.IsChatInitiated, true)
                      .Set(a => a.MatchStatus, MatchStatus.ActiveChat)
                       .Set(a => a.ProfileIdStatus, MatchStatus.ActiveChat)
                       .Set(a => a.MatchedProfileIdStatus, MatchStatus.ActiveChat)
                      .Set(a => a.IsProfileChatActive, true)
                      .Set(a => a.IsMatchedProfileChatHide, false)
                       .Set(a => a.IsMatchedProfileChatActive, true)
                      .Set(a => a.IsProfileChatHide, false) :
                      Builders<ProfileMatch>.Update
                      .Set(a => a.IsChatInitiated, true)
                      .Set(a => a.MatchStatus, MatchStatus.ActiveChat)
                      .Set(a => a.MatchedProfileIdStatus, MatchStatus.ActiveChat)
                        .Set(a => a.ProfileIdStatus, MatchStatus.ActiveChat)
                      .Set(a => a.IsMatchedProfileChatActive, true)
                       .Set(a => a.IsProfileChatActive, true)
                       .Set(a => a.IsMatchedProfileChatHide, false)
                      .Set(a => a.IsProfileChatHide, false);


                    await UpdateOneAsync<ProfileMatch>(matchProfile, update);
               }
               Message msg = new Message();
               msg.ProfileId = new ObjectId(message.ProfileId);
               msg.RecipientProfileId = new ObjectId(message.RecipientProfileId);
               msg.IsProfileMessageActive = message.IsActive;
               msg.IsMatchedProfileMessageActive = message.IsActive;
               msg.IsRead = message.IsRead;
               msg.MessageText = message.MessageText;
               msg.ReceivedTime = Convert.ToDateTime(message.ReceivedTime);
               msg.RecipientEmailId = message.RecipientEmailId;
               msg.ReceipientFacebookId = message.ReceipientFacebookId;
               msg.SenderProfileName = message.SenderProfileName;
               msg.SentTime = Convert.ToDateTime(message.SentTime);
               msg.Status = message.Status;
               msg.IsProfileMessageActive = true;
               msg.IsMatchedProfileMessageActive = true;
               await AddOneAsync(msg);
          }

          public async Task<(IList<Message> messages, int unReadCount)> GetChat(string profileId, string recipientProfileId, int skip, int limit)
          {
               // Getting all the un read messages between profile id and receipient profile id to update the read status


               await ChangeMessageReadStatus(recipientProfileId, profileId);

               var chat = await GetPaginatedAsyncByReverse<Message>
                              (
                               x => (
                                       (
                                              ((x.ProfileId == ObjectId.Parse(profileId)) && (x.RecipientProfileId == ObjectId.Parse(recipientProfileId)) && x.IsProfileMessageActive)
                                                   ||
                                              ((x.ProfileId == ObjectId.Parse(recipientProfileId)) && (x.RecipientProfileId == ObjectId.Parse(profileId)) && x.IsMatchedProfileMessageActive)
                                       )
                                    ),
                               skip,
                               limit
                              );
               var unReadCount = await GetUnReadMessagesCount(profileId);

               return (chat, unReadCount);
          }

          public async Task<(bool isDeleted, int unReadCount)> DeleteChatAsync(bool isDeleteChat, ProfileMatch profileMatch)
          {
               var matchProfile = await GetOneAsync<ProfileMatch>(p => (p.ProfileId == profileMatch.ProfileId && p.MatchedProfileId == profileMatch.MatchedProfileId) || (p.ProfileId == profileMatch.MatchedProfileId && p.MatchedProfileId == profileMatch.ProfileId));
               if (matchProfile != null)
               {
                    var update = matchProfile.ProfileId == profileMatch.ProfileId ?
                                                                 (Builders<ProfileMatch>.Update
                                                                    .Set(a => a.IsProfileChatActive, !isDeleteChat)
                                                                    //.Set(a => a.IsChatInitiated, false)
                                                                    .Set(a => a.MatchStatus, MatchStatus.ActiveChat)
                                                                    .Set(a => a.ProfileIdStatus, MatchStatus.Like)
                                                                    )
                                                                      :
                                                                    (Builders<ProfileMatch>.Update
                                                                         .Set(a => a.IsMatchedProfileChatActive, !isDeleteChat)
                                                                         .Set(a => a.MatchStatus, MatchStatus.ActiveChat)
                                                                         .Set(a => a.MatchedProfileIdStatus, MatchStatus.Like)
                                                                         );
                    // .Set(a => a.IsChatInitiated, false));

                    var messages = GetAll<Message>(msg => (msg.ProfileId == profileMatch.ProfileId && msg.RecipientProfileId == profileMatch.MatchedProfileId) || (msg.ProfileId == profileMatch.MatchedProfileId && msg.RecipientProfileId == profileMatch.ProfileId));
                    List<WriteModel<Message>> updates = new List<WriteModel<Message>>();
                    var updatedMessage = messages;
                    updatedMessage.ForEach(msg =>
                     {
                          if (msg.ProfileId == profileMatch.ProfileId && msg.RecipientProfileId == profileMatch.MatchedProfileId)
                          {
                               msg.IsProfileMessageActive = false;

                          }
                          else
                          {
                               msg.IsMatchedProfileMessageActive = false;
                          }
                          var filter = Builders<Message>.Filter.Eq(x => x.Id, msg.Id);
                          updates.Add(new ReplaceOneModel<Message>(filter, msg));
                     });


                    await MongoDbContext.Database.GetCollection<Message>("Messages").BulkWriteAsync(updates);


                    ////Insert or Updte matched profiles
                    var status = await UpdateOneAsync<ProfileMatch>(matchProfile, update);
                    int messagesUnReadCount = await GetUnReadMessagesCount(profileMatch.ProfileId.ToString());
                    return (status, messagesUnReadCount);
               }
               int unreadCount = await GetUnReadMessagesCount(profileMatch.ProfileId.ToString());
               return (false, unreadCount);
          }

          public async Task<(bool isHid, int unReadCount)> HideChatAsync(bool isHideChat, ProfileMatch profileMatch)
          {
               var matchProfile = await GetOneAsync<ProfileMatch>(p => (p.ProfileId == profileMatch.ProfileId && p.MatchedProfileId == profileMatch.MatchedProfileId) || (p.ProfileId == profileMatch.MatchedProfileId && p.MatchedProfileId == profileMatch.ProfileId));

               if (matchProfile != null)
               {
                    await ChangeMessageReadStatus(profileMatch.MatchedProfileId.ToString(), profileMatch.ProfileId.ToString());

                    int unreadCount = await GetUnReadMessagesCount(profileMatch.ProfileId.ToString());
                    var update = matchProfile.ProfileId == profileMatch.ProfileId ? (Builders<ProfileMatch>.Update.Set(a => a.IsProfileChatHide, isHideChat))
                                                                                          :
                                                                                     (Builders<ProfileMatch>.Update.Set(a => a.IsMatchedProfileChatHide, isHideChat));

                    ////Insert or Updte matched profiles
                    var status = await UpdateOneAsync<ProfileMatch>(matchProfile, update);
                    return (status, unreadCount);
               }
               int unReadCount = await GetUnReadMessagesCount(profileMatch.ProfileId.ToString());
               return (false, unReadCount);
          }

          public async Task<int> GetUnReadMessagesCount(string profileId)
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

          public async Task ChangeMessageReadStatus(string profileId, string receipientProfileId)
          {
               var messages = GetAll<Message>(m => (!m.IsRead) &&
                                                          (
                                                                (m.ProfileId == new MongoDB.Bson.ObjectId(profileId) && m.RecipientProfileId == new MongoDB.Bson.ObjectId(receipientProfileId))
                                                          )
                                              );

               await UpdateManyAsync<Message, bool>(messages, m => m.IsRead, true);
          }
     }
}
