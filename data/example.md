
Extraction for station 87113803 at 21h53
```
<?xml version="1.0" encoding="UTF-8"?>
<passages gare="87113803">

<train><date mode="R">02/01/2017 22:12</date>
<num>118622</num>
<miss>HAVA</miss>
<term>87281899</term>
</train>

<train><date mode="R">02/01/2017 22:17</date>
<num>118651</num>
<miss>TAVA</miss>
<term>87116210</term>
</train>

<train><date mode="R">02/01/2017 22:43</date>
<num>118817</num>
<miss>TOHA</miss>
<term>87116210</term>
</train>

<train><date mode="R">02/01/2017 22:46</date>
<num>118820</num>
<miss>HOTA</miss>
<term>87281899</term>
</train>

<train><date mode="R">02/01/2017 23:13</date>
<num>118837</num>
<miss>TOHA</miss>
<term>87116210</term>
</train>

<train><date mode="R">03/01/2017 00:16</date>
<num>118964</num>
<miss>HOTA</miss>
<term>87281899</term>
</train>

<train><date mode="R">03/01/2017 00:43</date>
<num>118933</num>
<miss>TOHA</miss>
<term>87116210</term>
</train>

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
    {'miss': 'TAVA', 'date': {'#text': '02/01/2017 22:19', '@mode': 'R'}, 'station': 87113803, 'num': '118651', 'request_date': '20170102T215830', 'term': '87116210'},
    {'miss': 'TOHA', 'date': {'#text': '02/01/2017 22:43', '@mode': 'R'}, 'station': 87113803, 'num': '118817', 'request_date': '20170102T215830', 'term': '87116210'},
    {'miss': 'HOTA', 'date': {'#text': '02/01/2017 22:46', '@mode': 'R'}, 'station': 87113803, 'num': '118820', 'request_date': '20170102T215830', 'term': '87281899'},
    {'miss': 'TOHA', 'date': {'#text': '02/01/2017 23:13', '@mode': 'R'}, 'station': 87113803, 'num': '118837', 'request_date': '20170102T215830', 'term': '87116210'},
    {'miss': 'HOTA', 'date': {'#text': '02/01/2017 23:16', '@mode': 'R'}, 'station': 87113803, 'num': '118842', 'request_date': '20170102T215830', 'term': '87281899'},
    {'miss': 'TOHA', 'date': {'#text': '02/01/2017 23:43', '@mode': 'R'}, 'station': 87113803, 'num': '118873', 'request_date': '20170102T215830', 'term': '87116210'},
    {'miss': 'HOTA', 'date': {'#text': '02/01/2017 23:46', '@mode': 'R'}, 'station': 87113803, 'num': '118918', 'request_date': '20170102T215830', 'term': '87281899'},
    {'miss': 'TOHA', 'date': {'#text': '03/01/2017 00:13', '@mode': 'R'}, 'station': 87113803,'num': '118909', 'request_date': '20170102T215830', 'term': '87116210'},
    {'miss': 'HOTA', 'date': {'#text': '03/01/2017 00:16', '@mode': 'R'}, 'station': 87113803, 'num': '118964', 'request_date': '20170102T215830', 'term': '87281899'},
    {'miss': 'TOHA', 'date': {'#text': '03/01/2017 00:43', '@mode': 'R'}, 'station': 87113803, 'num': '118933', 'request_date': '20170102T215830', 'term': '87116210'},
    {'miss': 'TOHA', 'date': {'#text': '03/01/2017 01:13', '@mode': 'R'}, 'station': 87113803, 'num': '118967', 'request_date': '20170102T215830', 'term': '87116210'}
]
```
