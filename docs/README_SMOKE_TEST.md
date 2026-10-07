# SRE Smoke Test

Bu dosya küçük ve kontrollü bir SRE senaryosudur.

Beklenen hikâye:

`payment-api -> primary DB timeout -> retry -> order-service dependency failure`

Bağımsız gürültü örnekleri:

- inventory-api health check 200
- worker background job completed

Test ekranında aşamaları tek tek seçip yalnızca seçilen aşamanın çıktısını inceleyin.
