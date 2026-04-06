// <copyright file="FileRepository.cs" company="Wowzer">
// Copyright (c) Wowzer. All rights reserved.
// </copyright>
// <author>Shiva Kumar </author>

using Microsoft.Extensions.Options;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Wowzer.Services.Models;
using Wowzer.Services.Repository.Interfaces;

namespace Wowzer.Services.Repository
{
     /// <summary>
     ///     The File Repository
     /// </summary>
     /// <seealso cref="IFileRepository" />
     public class FileRepository : IFileRepository
     {
          /// <summary>
          ///     The connectionString
          /// </summary>
          private readonly IOptions<Config> connectionString;

          // private readonly string containerName = "profileimages";
          //private readonly string containerName = "profileimagesuat";
          private readonly string containerName = "profileimagesprod";


          /// <summary>
          ///     Initializes a new instance of the <see cref="FileRepository" /> class.
          /// </summary>
          /// <param name="connectionString">The azure connection.</param>
          public FileRepository(IOptions<Config> connectionString)
          {
               this.connectionString = connectionString;
          }

          /// <inheritdoc />
          public async Task<Dictionary<string, object>> UploadFileAsync(string fileName)
          {
               var fileData = new Dictionary<string, object>();
               FileInfo fi = new FileInfo(fileName);
               string allowedResumeExtensions = connectionString.Value.AllowedExtensions;
               if (!allowedResumeExtensions.Split(';').Contains(fi.Extension))
               {
                    return null;
               }

               CloudStorageAccount account = CloudStorageAccount.Parse(connectionString.Value.StorageAccountConnectionString);
               CloudBlobClient client = account.CreateCloudBlobClient();
               CloudBlobContainer container = null;
               string blobName = string.Empty;
               blobName = Guid.NewGuid().ToString() + "/" + fi.Name;
               container = client.GetContainerReference(containerName);
               if (container != null)
               {
                    var cldBlob = container.GetBlobReference(blobName);
                    await container.CreateIfNotExistsAsync();

                    SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy()
                    {
                         Permissions = SharedAccessBlobPermissions.Write,
                         SharedAccessExpiryTime = DateTime.UtcNow.AddMinutes(5)
                    };
                    string sharedAccessSignature = cldBlob.GetSharedAccessSignature(policy);
                    string containerWithSasUri = new UriBuilder(cldBlob.Uri) { Query = sharedAccessSignature.TrimStart('?') }.Uri.AbsoluteUri;
                    fileData.Add(
                        "blobData",
                     new
                     {
                          sasUrl = containerWithSasUri,
                          blobName = blobName
                     });
               }

               return fileData;
          }

          public async Task<Dictionary<string, object>> UploadBlobAsync(Microsoft.AspNetCore.Http.IFormFile file)
          {
               var fileData = new Dictionary<string, object>();
               FileInfo fi = new FileInfo(file.FileName);
               string allowedResumeExtensions = connectionString.Value.AllowedExtensions;
               if (!allowedResumeExtensions.Split(';').Contains(fi.Extension))
               {
                    return null;
               }

               CloudStorageAccount account = CloudStorageAccount.Parse(connectionString.Value.StorageAccountConnectionString);
               CloudBlobClient client = account.CreateCloudBlobClient();
               CloudBlobContainer container = null;
               string blobName = string.Empty;
               blobName = Guid.NewGuid().ToString() + "/" + fi.Name;
               container = client.GetContainerReference(containerName);
               if (container != null)
               {
                    var cldBlob = container.GetBlockBlobReference(blobName);
                    await container.CreateIfNotExistsAsync();

                    SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy()
                    {
                         Permissions = SharedAccessBlobPermissions.Write,
                         SharedAccessExpiryTime = DateTime.UtcNow.AddMinutes(5)
                    };
                    string sharedAccessSignature = cldBlob.GetSharedAccessSignature(policy);
                    //string containerWithSasUri = new UriBuilder(cldBlob.Uri) { Query = sharedAccessSignature.TrimStart('?') }.Uri.AbsoluteUri;
                    fileData.Add(
                        "blobData",
                     new
                     {
                          sasUrl = cldBlob.Uri.AbsoluteUri,
                          blobName = blobName
                     });

                    using (var filestream = file.OpenReadStream())
                    {
                         await cldBlob.UploadFromStreamAsync(filestream);
                    }

               }

               return fileData;
          }

          public async Task<bool?> DeleteUri(List<string> blobList)
          {
               CloudStorageAccount account = CloudStorageAccount.Parse(connectionString.Value.StorageAccountConnectionString);
               var client = account.CreateCloudBlobClient();
               var isDeleted = false;
               var container = client.GetContainerReference(containerName);
               if (container.ExistsAsync().Result)
               {
                    for (var index = 0; index < blobList.Count; index++)
                    {
                         var blob = blobList[index].Split(containerName + " / ");
                         var cloudBlob = container.GetBlobReference(blob[1]);
                         if (cloudBlob.ExistsAsync().Result)
                         {
                              cloudBlob.DeleteAsync();
                              isDeleted = true;
                         }
                    }
               }

               return isDeleted;
          }
     }
}
