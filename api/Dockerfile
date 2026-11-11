# The SignalR browser client is published on npm; fetch it once at build time
# so the page does not depend on a CDN.
FROM node:22-alpine AS js
WORKDIR /js
RUN npm pack @microsoft/signalr@9 --silent && tar -xzf microsoft-signalr-*.tgz

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
# Restore first, from the project files alone, so this layer is reused until a dependency changes.
COPY Rivus.Api/Rivus.Api.csproj Rivus.Api/
COPY Rivus.Api.Tests/Rivus.Api.Tests.csproj Rivus.Api.Tests/
RUN dotnet restore Rivus.Api.Tests/Rivus.Api.Tests.csproj
COPY . .
COPY --from=js /js/package/dist/browser/signalr.min.js Rivus.Api/wwwroot/lib/signalr.min.js

FROM build AS test
CMD ["dotnet", "test", "Rivus.Api.Tests", "--nologo"]

FROM build AS publish
RUN dotnet publish Rivus.Api -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=publish /app .
ENV ASPNETCORE_HTTP_PORTS=8080
USER app
ENTRYPOINT ["dotnet", "Rivus.Api.dll"]
