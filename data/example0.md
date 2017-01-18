
Extraction for station 87113803 at 21h53
```
<?xml version="1.0" encoding="UTF-8"?>
<passages gare="87113803">

<train><date mode="R">02/01/2017 22:12</date>
<num>118622</num>
<miss>HAVA</miss>
<term>87281899</term>
</train>

...

<train><date mode="R">03/01/2017 01:13</date>
<num>118967</num>
<miss>TOHA</miss>
<term>87116210</term>
</train>

</passages>
```

Json format with request date and station:

```
[
    {'miss': 'HAVA', 'date': {'#text': '02/01/2017 22:12', '@mode': 'R'}, 'station': 87113803, 'num': '118622', 'request_date': '20170102T215830', 'term': '87281899'},

    ...

    {'miss': 'TOHA', 'date': {'#text': '03/01/2017 01:13', '@mode': 'R'}, 'station': 87113803, 'num': '118967', 'request_date': '20170102T215830', 'term': '87116210'}
]
```
