// <copyright file="ExceptionFilter.cs" company="Wowzer">
// Copyright (c) Wowzer. All rights reserved.
// </copyright>
// <author>Clayton Fetzer</author>

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Security.Authentication;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Primitives;

namespace Wowzer.Services.Filters
{
    /// <summary>
    /// An API filter used for handling API exceptions.
    /// </summary>
    public class ExceptionFilter : ExceptionFilterAttribute
    {
        /// <summary>
        /// The log
        /// </summary>
        private static readonly log4net.ILog Log = log4net.LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        
        /// <summary>
        /// On API exception, will log error and add an error message to the response's headers.
        /// </summary>
        /// <param name="context">Context on which the exception occurred.</param>
        public override void OnException(ExceptionContext context)
        {
            var phoneModel = string.Empty;
            var operatingSystem = string.Empty;
            var wowzerVersion = string.Empty;
            var headers = context.HttpContext.Request.Headers;

            if (headers.ContainsKey("Model"))
                phoneModel = headers.First(x => x.Key == "Model").Value;

            if (headers.ContainsKey("OS"))
                operatingSystem = headers.First(x => x.Key == "OS").Value;

            if (headers.ContainsKey("Version"))
                wowzerVersion = headers.First(x => x.Key == "Version").Value;

            Log.Error(
                $"Exception: {context.Exception.Message}, PhoneModel: {phoneModel}, OperatingSystem: {operatingSystem}, AppVersion: {wowzerVersion}", context.Exception);

            if (context.Exception is InvalidCredentialException || context.Exception is UnauthorizedAccessException)
                context.HttpContext.Response.Headers.Add(new KeyValuePair<string, StringValues>("Error", "Authorization Failed. Please provide a valid username and password."));
            else
                context.HttpContext.Response.Headers.Add(new KeyValuePair<string, StringValues>("Error", context.Exception.StackTrace));

            context.HttpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
            context.HttpContext.Response.WriteAsync(context.Exception.Message + "/r/n" + context.Exception.StackTrace);
        }
    }
}
