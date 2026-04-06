// <copyright file="LogFilter.cs" company="Wowzer">
// Copyright (c) Wowzer. All rights reserved.
// </copyright>
// <author>Clayton Fetzer</author>

using System;
using System.Linq;
using System.Reflection;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Logging;

namespace Wowzer.Services.Filters
{
    /// <summary>
    /// Used by all api calls to ensure proper logging of entry and exit.
    /// </summary>
    public class LogFilter : ActionFilterAttribute
    {
        /// <summary>
        /// The log
        /// </summary>
        private static log4net.ILog log = log4net.LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        /// <summary>
        /// Logs the entry of an API action.
        /// </summary>
        /// <param name="context">The http context.</param>
        /// <inheritdoc />
        public override void OnActionExecuting(ActionExecutingContext context)
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

            log.Info($"Begin Action {context.ActionDescriptor.DisplayName.Split(' ').DefaultIfEmpty(string.Empty).First().Split('.').DefaultIfEmpty(string.Empty).Last()} - PhoneModel: {phoneModel}, OperatingSystem: {operatingSystem}, AppVersion: {wowzerVersion}");

            base.OnActionExecuting(context);
        }
    }
}
