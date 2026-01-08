Модуль qtx-go реализует поддержку локальных транзакций с множественными участниками, взаимодействие
с которыми происходит по протоколам Two Phase Commit (2PC) и Single Phase Commit (SPC).
Участниками транзакции являются диспетчеры долговременных (durable) и не долговременных (volatile)
ресурсов.

Модуль реализован на основе идей пакета .NET [System.Transactions](
https://learn.microsoft.com/en-us/dotnet/api/system.transactions?view=net-10.0) и предоставляет
подмножество реализуемого им функционала.
