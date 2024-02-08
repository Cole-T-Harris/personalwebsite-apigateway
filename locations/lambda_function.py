from pydantic import BaseModel
from typing import List, Optional
import json
import requests
import boto3
import time
import geopy.distance

BASE_KROGER_URL = "https://api.kroger.com/v1/"
SCOPE = ''
TOKEN_KEY = 'locations'
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table_name = 'OauthToken'
dynamoDB_table = dynamodb.Table(table_name)
THUMBNAILS = {
    "BAKERS":            "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAAe1BMVEX//////PzxU1buHiX0hoj+8vL94+TzfX/vMjfvOz/83d7tAADybG/1jY/5wsP+9/f1kZP72NnuGB/2n6DyZ2ruJSv97OztAA74sbPzenztEBn6yMn6ysv709T4ubrwQkf3pqjuLjPycHLxWFz2m53xX2LwSk7vPkP96OggG/7EAAAAxklEQVR4Ae3NNYLDMBAAwLWFRp1YZob/f/AoWKVLl2mkZfh4jyiC12L01IxvP0IpJZcv43CHk9svzfIiKwHIrUHAny95uyeUBm4Ix9axRPrAqjhFpi6Maa4NqChR2xVQ93goZKlCPU7MTbNIrw12ZkMKYZl6zFG/WuoIWkGryV0aUqRh45gL1eO9SkYlobUryNbqS8M0HbBOi93VOKPTs0FZ6jHwzspLAxpLWKxoI/lLpJ1oCJESXO/gohzChlp4QXQS3u3jB/UCDPBTXuNfAAAAAElFTkSuQmCC",
    "CITYMARKET":        "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAAwFBMVEX////S2trJ0NH3+fnQ1tbLzs78/PzJ0dHW29vd3d319vbV1dXMzc30+Pi6oZ/sU0v1MiS4qqnX5ebgd3LIm5n3Y1vmjor1fHfDxcT8VUrg4+O3x8j9dm+sp6b0Iw7zdG34pKDla2Xcgn3vAAD4k46ot7jJkY78wr+3gn/o9PSlq6rWhoPMXlnKoqD1Oy/1hoD2j4nyaGHu1dPzWVHuMCLjmpfwJBDtqabsSkD5trP92NbiNivRgn+ziYfmW1OtjYzfoEGyAAAAvUlEQVR4Ae3Cg5UDAQAFwL9GbHNtW/1XdWYFh2Te4O6XIUgKNMOC5RieEwCIZA8U18erwXA0nkxncx7kYr5cceDE9Wa93e0PAl4Mj6fz5SrJylaVNEk3JmPTsqeOK614POMvEst6fuDPwih2o+TipZlrWXIwX+MFHeVYFXHkuaWUppG8WAWeFEV+xeGVUUv5KDgHkrY8e5c6yJt2VfgjT9/izbZPEwTfjRWeIIgtQVBbguef4htxMqHwq909AhK0E1YFHEUGAAAAAElFTkSuQmCC",
    "DILLONS":           "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAAt1BMVEX////+//////3//f/8/vz7///8/Pz/8/P+/fn329fkg4bOJjDWS1vkjpb4vbnZe4D00M/99fj2y8jRLkDaaWv1xMDJABb00tb36+rrp6XZV2bcjpbz19vJAAD48ezLBCTheoPmk5jPOT7TVlv55ubhoKjrs7naY2zrxcjKHSbpt7nKAB3kpqfVX2PmnZ7ca3bdVFbdfXzjlJDVSVLWKD3rsq/ROUXlmqLDx8i2tbLk4OC+wcTUz9KQ/2hvAAABG0lEQVR4AWJAAqMA0P44JUgQBEE0onqyxrZttnn/a63t3e99xXQmqW54+hN852DgkZSSVEq9z6HTmUdnUPjaiGwuXyiWyhVhFbV6o9lqAzQUHjJ1ur3+QOtOcZgdjSf56WzeWAAQEIq4YbnSXNQqxLq82eZ3jX1lIneJDyPeZwD0YLIQ5o7I1k+z8/pSHxy6h2wrTeKewbFNwxgfTGtc7OfN8skebp1zt60eHNS87HZk3fLM3CxbP05sazNYVVZ+Rx4G6+7tyzw3bu7nm+KqcnLLuRsO8jilpIdeVXnrNBYV/2ivT7vFSIPggwe5aWUp0O5xW+guNCQIwigI42SERwSgHOc7/SBrre/OMxTJdEil8AkEAUNI/JF/rgFv+RbCLiRGqAAAAABJRU5ErkJggg==",
    "FOOD4LESS":         "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABIAAAASCAMAAABhEH5lAAAAbFBMVEX////T09O/v7/4+Pg6OjoAAAAfHgBAPQAyMAD8/PwTEgDk2AD/8gC6sACurq7AtgCWjgCvr696dAB2cAA7OABWUgA2MwDv4gAVFACBegBGQwD36gCHgADc0ADWywAoJgBkXwANDQAzMzPx8fFTKN6kAAAAiklEQVR4AVXOhQGAMBAEwcftcHfrv0ceiyw+QBIiMkwlg8GynS/X83zHsQMyfwkjIOarSclPKYCMr7mgAlypUoW7WqF7IK5RKEUbM3WSemBgGh1BHjA5M7AIClsgTQHMffdRA9FPa8otDHOsLXVj2hyV9pSpeMl8JINYvUnBcZPvPfHoR0B0mlonXVHpC/c0g9RZAAAAAElFTkSuQmCC",
    "FRED MEYER STORES": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAAb1BMVEX////83N34tbb3sLL+8PD5vL7+7e74s7T84uPzd3rwWFz1lZf719j+9/j//Pz60NHrAAD6yMnuICjwTVLvP0T1kZPuLDPze3/yam7uMDfycnbuJCzxXWHzgYTvOD/5wsTvRUr3o6b0i473qar95+j/eaZoAAAAgElEQVR4Ae3KtQEDMRAEQMkvZnhm6r9FkyJTbprkYBf8fRZ4yFDaMKGEUXCPC4mVxgZZ550XwWCiELMKQZgKMS+cKKu6aUscqq4vqzIOYxu4m1LBTXhmnazC1Bfc8XGUSxBxrWaccl0Du4GKIz7gxReZH/juvO+xtDt4AUvw94FOXasHgN2RFWEAAAAASUVORK5CYII=",
    "FRYS":              "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAA1VBMVEX////5xMX2oaP5yMn59vbuNDrsAADxAAnSXV/yeXztAA7jGCDFISfzDhrUAADZ4uHwGyO0AAD26urSWFukd3j97e73sLH2qKrOCxX4uLr6wcP6sLLmyMnzYGTtHCT0VFr2hon4nJ/1dHf3kJP0bG/gAACwra3/9fXLZmnKcnTEhYbHf4HIj5DOmpumZmi1AAb2S1DdfoHaUVXYP0Tgo6TiBRLk6em3MDSxChPHAADlr7G8wMCORUfYysr14OC3u7vuMDfZNTrWZmnxAACSFxvCrKzOpaZ/HqPlAAAA7UlEQVR4Ae3QQwLDQBQA0B8NG9W2bRu5/5Fqe583NuBPNkGUPg3LCsKIwHuUcYeq6fAWM0yny/1u1OP1McMvSEJgF7yBoBAIBoNCMBgKw1EEoWhUpbF4IinGI5F4PJVOxBMRMZM9jufyhbxq+IulcrFQqpTKu1QsVyplUoWLHDJqoNRjTGnUm1K9rsdabaOTUbqXW+wmyEjt4UxfHQwRGgXG2GgOJ/LDBPd0Flf9Q4ZGAHOm4gU8TgBIMZ429hN87eWuvE5YmTJa7SZMEdb2A57OCq/WcDGZjHZp/5PTTXuVme27rCy8tJuig+1vW8I+Ghglm6BIAAAAAElFTkSuQmCC",
    "GERBES":            "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAAk1BMVEX////88/T23d/66+z22tzpr7PFAAD77/DUTlfhjpPoqq7IAADJABH55+jmoqbvxcjuwMPRP0n34uThio/bc3nXXGTPLTrOJzXegojRQUvgi5DdfoTRNEHvxMbilZnrtrnXYWjLBh7UUFn11tjZanHieoTVV1/NHi7V1taQkJCmpKWJhoe+u7x0cnJ9fHz6+vrl4+RWh6C4AAAA6klEQVR4Ae2QQ4IEQRAAs1XGWG0b///csrE4znWiXOmEF09gmP//LHu7OwgZf8SYIESXB0OM/5ULIdWiYCO9+76belHY8wMcreVxss7ALlcD6G3+2iH15fquH660kfJ8HwXHEKJ4VkjE555qfc0ICnmkxF0oZLkazQrIhSAjeZpGGVGMw40V0SE9XlAxV00VRDy4qINz4KXL4yPgpFCAQzwrhPaHQnoscwkHC5xbSE+nU0YN7cwhsl1+vVpg6/BG4vuNafm3DcwE01Y8PjkHAKjqpu3qvhnaBlbC0pEYZsZphPELWDHhxZO8A0pWEEyQ1xSsAAAAAElFTkSuQmCC",
    "JAYC":              "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAA8FBMVEX////86OXlY174v77kionYAADlUU7xp6P60s762tf74N3/7ur89PT/9/fts7LWGhXWJyTTFBDQAADrqqnGJyTTIBvXLSnaPDbbREHkaWnldnXtmJfz4ODWsbG7NzfPJyfFJyzAAAzdkpPKVlidAADo4+Pk1dW/AADKHiDcycmpo6PDYWDNm5vaV1fpubm5aWnLNzuRfn7XZGXHdna/RUbUu7u0IyeAVFW2AADZe3x2JSeyFRjIiIh3AAC7fn3MPD/Jj4+MAADLysq7MzOuFRiqGh+ZmJjX19eboqK8u7uHjo6HhYXEw8N6d3fS0dHW1dVm1VciAAABM0lEQVR4AeTKtQECMQAAwOAO8bzHcHd39t8JSpwBuPrA/4iBH+IJ8F0ylc4ksrl8oVh6H8oVWEGYFCiDkAsnHXc9P7gPOcw5zoYgkpJSpRTVxlZrd6GY4syv41qDSdpUVDWDsNVogzuS42LH6/b6lhltG9nBsKtH92EMUW7SN6GZlmfjlq3bYK4W98FUOE7Wg6iuWo3aMjdYTaha34c+YiuwWYXlQbm13YJNU03V7j4UMQQhgoxJ3N83ip3DUalT7X4QUddQ3jTruukFXapU9Qzu9cfR4RBFl8t8fi13jo0AhGEYAHoAxiCNJqD2Eccixuy/DTkq0rAAr1Mt7XXba62rLTJpxd05Ks5jxDss5EXVBsAENPDs3TitNC4BAhBEpIWaokxHS7bH9fTKVGaTb1T5ixv/VDGqvII3WQAAAABJRU5ErkJggg==",
    "KROGER":            "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAAeFBMVEX////o7PSfr82KncOxvtb7/f6drcwAGYU7Y6Nng7QARZUAN5Dc4u2+yd0AHofM1eUALYz09/olVp0zXqCVp8jS2ugAJonc4+12j7tPc6zt8PZuh7Zde6+Bl7+uvNUKS5gAQpQgU5xFaaYAM40AO5HCzN9ceq8AEoRulIj8AAAAyElEQVR4Ae3NhXHEMAAAwbOkl5mZ2e6/w0wY1UG2geW7f5aQ6oGZth3X8wNMwigGZJJikGme5QV/KEuoap6Jhj+4LV3UYdYPjDm6zadg9gXxsmZhvfrTY2kmnvVbXwUUu7KO4ozqQ5d7uMrTDna341mzNEtBsZHaKZ6qLGZ1XH0TJ9ZHMd11kcOWXzvuvPnnsGk3vQNeKAV6igU8tLQgPDOFHktkx09px7NMYSA8nh0nBl0UAsKvUwyU3YsritMUk6BpR4t/vzwB+d4M8BLCv9kAAAAASUVORK5CYII=",
    "METRO MARKET":      "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAAZlBMVEX///+Zh4TUzcyXhoN+Z2O5rqx2XFfg3NuIc2/X0dBYMy20qabCubeaqoGuupq2wKTw8u2/tbOejov29fVwVVDIwL6PfHnr6OdoSUTT2MnN08GRo3Tm6eGsoJ7w7u5efiSisYtsT0ouTLQJAAAAqklEQVR4Ae3QRQLCQAwF0D8aQr2pe+9/STY4bHHeuAte5E/hLm2s80QbxdsgMKGyDERxkqSHCWFGuZiiNKVFVbsSDaK2S/vhOKGkUYzVpRSopBynAvOApcMB0cRSyhhAB1oQ5lPULvGAu9yhIJyQy9lMbJhd6BFMKqNtiDPhWFZkiNcggN3Iyqb0ODhesvK0BozGcKPDqbj8B65LCcY8Lwk5FFUGY44n+9sB+t4JauvzzH0AAAAASUVORK5CYII=",
    "PAYLESS":           "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAA3lBMVEX////k3+LX0dXq5ei6ubldkW8Afi8AmT0AeSkAcSE6UEHX1tYnd0YAoUIBl0oAqVIBdDkBgD4AjUVffmn/+/sAoEsBg0BSfWBnj3T+9vatsq4AWAAleUcDTyZ0ln+ruq+BoIs7elLHx8cVYzdwfXWTq5x7qJGdqqL1mJrwVVnuLTPzdnkAWyP96er4r7H6cnf3Q0vtHCTtFyD3p6n4ubvsAADsAAvtDRntGyPvPEGETi7bAAD709TtCBXsABDxX2L71tfzen31kpT2oKLwSk/5w8T6y8z83N30iYvxWl6l3k1WAAABnUlEQVR4AbROtQGAMBB8XOOO6/4r0kdKrjyHv5DleZFWy6qum6bt+iEeHhEmlPFGyLheK84Y1srEuq2EkTDGNFPMhfo0L5AjvTKNSR376LYdjporQpqzgCt24X7eT6pqagrqQOeLsABaI6PkCkEYirKI9+MOVAvW6CXYKoKK2P1vqDjV2tfpZ09IwkwuMyF5FmR5UZbVi5Sqfm2IilY800Gz6entPWUyxjB4eK6bBCUzTZM8HcIPhYRpvuzgvJO+m6jG82HyO+r8qj9AAAOamYwmGtmyBbi8BAMKIadMOv+YIagV2SwWJ1Zcw1Tg4NsQtkLWGFB13Pk1TivkKWg1pNu30A6PGh0moaasCgLfAqWta32UTSV7RCxKc7sj07i+EVDGj7juW56jj4Nywbk2dujvMcze+yX3qhyrevd75X2lln4XF/2yuMUPubMkt9SsUmGaVR9/TNqONNrRmobYWktMDfMqbnoy54ATR0ggipsIc22hOR2z+LXOr8qpM5jEMwF8SIjpqDO8+I2ckOCakCgG8Qdyc/JIzmXiP/kEKblgPWJ3d9QAAAAASUVORK5CYII=",
    "PICK N SAVE":       "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAAXVBMVEX////K18Xe59uAh49vd4CXnKK/wsagpqtnlFD1+fPl5+h1fYXu7/BlbniOlZt4gIiXnaPf4+BfaHPa3N6BiJCorLGHjpYfMkNUX2qvs7fh4uROWmbT1dfHys23ur60LUT3AAAAZklEQVR4Ae3BxQFCQQxAwQe7Sb67W/9dohesAGyGv0+12/OK86KAQRBC5OKEeylZrHmWFVFZpYkVNd411naScRH1PisqGIRorJVJ5gXJ82F0XHmw1We21dFWr2vqI5+3qls38vddjtdHBGfVCJPBAAAAAElFTkSuQmCC",
    "QFC":               "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAA0lBMVEX////W5fqyzeunwtKhvcOnv76btrGqsmzWyDL33AD/4wDQxTihrnHQ4fj0+f+cvNX/5QD/5Aj/4hX/4RqTrZrE2fSus13y2ibbzUTfzSGJq6/q8v+RqIrt1hHj0TmdrW3LxFPBvEL/6ADDwFqttGgodqwAY70AYL8AXcEdcrB2lo6OpXqryOX/4AD/5wBskZMAareaqnZdipsAWsLo1TGlv8hEgKOOo4DRyE13l4z/6gAAbbMAVccrd6oAZrlThp28vF+otGKst1q8v1GLpnOjuKgSACJ0AAABc0lEQVR4AdWQBYICIRRA7Q5iBZtRpixGlN3B7vtfae32AD4aHvUDX0cwFI5EI+FQ8O1qKBZPJFMHkulMJPu0msvHkwAifAJBkPgp3K/nCaBFSiG+gkCpXLleXa3ViyxTNOIY3yskdFovNJrctGzHbbU7+B5Q6h6Fbg8Lr1+VcjBU6NH4vQgYmX8Z39MCP5C6CmI05GLsuUV05PCCBwFpd9LEqOhYSFGlFEWwSCG9CaBqDw6n247ypwS3RqrPizMfXgXRtr29gOZ2MeNqIv2Z1wSycxOAby8ERtR2aMaJ21wMfYAhvHtD0Z0D2CTObJxxDa7FfLT31f0v/GHVlFZfQTnlAsGMq6Ch0E1AY+0xy24P0kuTIwjHRcnxnYCKMsMyrOXOWCaTkXsOjQnvTqAYUwX1ocW0Rp8jiQzCjfiKpZdrzTec7CuyWen7K1Zku2Wcc71mTC9XG8Yay9VVSAFRBEIIlBSH5lCOdQqchMLuI+HAd/APylxH8Zakog4AAAAASUVORK5CYII=",
    "RALPHS":            "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAAdVBMVEX//////fz75uT3zMjzuLLwoprvmZDtiX/uj4bysav1w7764N387OryraboZ1niIwDjMhXlQyzkOyLjNRriLAjmUD3nW0vwn5jqcWTkPybhGAD40s7nV0b1vrj52dXreWz98vHoY1Tvm5LsgnflSzbgAADreGuWdoz7AAAA0ElEQVR4Ae3PxYHDMBAAwLUFBjEmZkF8/Xd4zAXklXkuLzzcX9MiTGjXUzKMrIF/uBBEKq3NaJ3zIfh4YfCjvUaQASatOpidCiCVU3NY4NOonOTzCnRD/b4JfjQkXYeFzvQjn9TkxBjbbcxpK1AXBKFpye1q1ceMVhm/C1CtLJjLdDI2xCan5jb5Dd5Vr8CnDLkZQKBbYq/zxdkEHeDT5UAz5eOON8YYyrujaMd5rj/PJrKeCM1en+kpOG/9fFrawh9pxDWbXNRk4lE2Dg/39gKsjA9iyVpQoQAAAABJRU5ErkJggg==",
    "FRED":              "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAAb1BMVEX////83N34tbb3sLL+8PD5vL7+7e74s7T84uPzd3rwWFz1lZf719j+9/j//Pz60NHrAAD6yMnuICjwTVLvP0T1kZPuLDPze3/yam7uMDfycnbuJCzxXWHzgYTvOD/5wsTvRUr3o6b0i473qar95+j/eaZoAAAAgElEQVR4Ae3KtQEDMRAEQMkvZnhm6r9FkyJTbprkYBf8fRZ4yFDaMKGEUXCPC4mVxgZZ550XwWCiELMKQZgKMS+cKKu6aUscqq4vqzIOYxu4m1LBTXhmnazC1Bfc8XGUSxBxrWaccl0Du4GKIz7gxReZH/juvO+xtDt4AUvw94FOXasHgN2RFWEAAAAASUVORK5CYII=",
    "SMITHS":            "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAAe1BMVEX//////P3pnqzfZX7jfZLdXnj98/b87/L++vvNAA3vucPxxM3MAAD32+HOAB3sq7fSAD/SADr0z9b65+vnk6PrprPmi5354efhdYv11NvQADHPACXqorDbUW/UF0ngbYTWLVTUG0vzzNTOABrwvcftsb3SAD7ZQWTaSmoUdmErAAAAu0lEQVR4Ae2PNQLCQBBFf2xdiLsnwP0viFtFmSpvdXwGm7Lj4D+u5wd4Qx4R9NfOuID3FqQiINpISPv1OIRRDCCm0CAJ0iw3MhdFHL8dyqpu0HZ97Q2BHkXYl1UwZraV6duDDvUUBDCRqOk8dQ2dSzss80peZgZSFd7NYVoraqbOpXOUDki4+3SIE6Dzvw6HlNwdYiThy6EtvCM5nZla0lmrpR8sl0mX2N57T8vi23HegMLB7WHx7d3ZjiuDCQw1ZnElUwAAAABJRU5ErkJggg==",
    "HARRIS TEETER":     "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAA1VBMVEX///9CnH2/2c/Q5d1prZJ9t6HJ5d/5m57+8PD3+fwAg1IAfETd8u70bG799vb5v8D60dHe6+n1kZPtAADyZWjuHybycXLvKzD5xMT85eX72dn4trb2oaLzd3ntr2jolA7rplH217fzgoLi6fP33cPqnTj++fP88uj10a345MzwVFe2x+PU5fVfisc/eMA2c76Kp9Tzy6HyxZTwR0yas9mPcaProkX66deckLi+zOXtrF7xwInvuXZqksuOqtXnjADpmCOnvuF3k8Hdz8O9vMPJ1ers494KSBuIAAABIElEQVR4AdTBxQGDAAAAsasXd3f23xF3+5NwN48nl17vz/f355wgSrKCqumcUQ3V1CzbcTlmeq7vOQF4PkdCD9QoAOKEtTTLC4hKBppdVz/XiA3EQABFf6sq6cKoZdCCQYZluv+NQmbs88rhASEFG4Zp2c67e+sB+D7cvAShUlG8LUgAO7VGD8A4hudJLEGMQ1asKdbrh63teyAOYRbwZx6zsnCTZZImSbaEXBRCrtKCQvLHKi0NaK0zYoRgQ/qs6ApsE50tjBrJhmj2HsFafnzYtMG4Fpu8Emx9VAAkXQPjTb5nj/NmgZWFw1LLXAJzVXPAKF0YS0i0VCpUas4RXab8WULg95yyKtMC0AaXLEsHIOMie2pqqLjCXZhvKf/NNzxJIiZofUNVAAAAAElFTkSuQmCC",
    "KINGSOOPERS":       "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAA8FBMVEX////hAAD97u7sdHf+9/f3y8z98vLzsrPrc4TscnXte3775eX++vr/8QDsdoPxlpnviYz1vL7rZWnyn6H/+QDjFyD5zwD83QDzrK7wkpXteXzwkJLrbG/ypKbug4X2xMX63N3mRkrkISj409TqX2PnNj3oUlb1vMTvjpvynwDugRDscXvlMSbrbRjgACPoTyL60C70qzjqaoz1s0v1sij0rIXym4P1sYH4wlDxloX5yWn60k73v3/2vVXpXHTzqSH1tQD95gD3wwD60wDuhSLqXR/wkRXxnBvnRxrxk1zraF3td0zpVGjtfVnqZgDpWgBtA/IGAAABT0lEQVQ4je2S2XKCMBSGj8SFoCwikKrY1qW2Eou7di+L3ez2/m/TExSmF/WqV53pN5kw5/CTLwEA/tlRzO3lIAnkqVLXjYauG7qu4DD0eqOpY6mQwjbgumWotgAOAUyAagnyRPSPKC2kijIcA7TbAJ0KgARFzHW7zVyutF2BkDJepJqCK1RERzIAerWW0dgpTFMEoILP9bLACSrNb4puKemhAptCgfRTBaUSnJ4BuMkma1XcdnKDZqcgvD9wPE4ElFAqZkJc4qYKe2gNzi3ftm3fHo582xfgnCm08WQ6my+W2nIljy8ulavF+PpGXq14quC3qnd3H6ieE3pW7ESxE7PZ2prxTNF5UB/ZSFWHFpvPmfU0CNiasedUwV/UkG2i8HWCMcd5c1i8YVEQ8PQUVJFlWdE0HO/hVEsqWcOaZC8q4+Pzh88N0l7yv/7Z/gpfLtEgv73hjUcAAAAASUVORK5CYII="
}

class LocationsBase(BaseModel):
    zipcode: int
    radiusInMiles: int
    limit: int

class Address(BaseModel):
    addressLine1: str
    city: str
    state: str
    zipCode: str
    county: str

class Geolocation(BaseModel):
    latitude: float
    longitude: float

class DailyHours(BaseModel):
    open: str
    close: str

class Hours(BaseModel):
    monday: DailyHours
    tuesday: DailyHours
    wednesday: DailyHours
    thursday: DailyHours
    friday: DailyHours
    saturday: DailyHours
    sunday: DailyHours

class Store(BaseModel):
    locationId: str
    chain: str
    name: str
    address: Address
    geolocation: Geolocation
    thumbnail: Optional[str]
    hours: Hours
    distance: Optional[float]
    
    def to_dict(self):
        return self.dict()

class LocationsResponse(BaseModel):
    zipcode: int
    radiusInMiles: int
    limit: int
    stores: List[Store]
    
    def to_dict(self):
        return self.dict()

def get_distance(store, zipcode_lat, zipcode_long):
    starting_coordinates = (zipcode_lat, zipcode_long)
    store_coordinates = (store["geolocation"]["latitude"], store["geolocation"]["longitude"])
    distance = geopy.distance.geodesic(starting_coordinates, store_coordinates).miles
    return distance

def get_thumbnail(branch):
    if branch in THUMBNAILS:
        return THUMBNAILS[branch]
    return THUMBNAILS["KROGER"]

def make_lambda_request(lambda_name, payload):
    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    response_payload = json.loads(response['Payload'].read().decode('utf-8'))
    return response_payload
    
def get_oauth_header():
    dynamo_response = dynamoDB_table.get_item(
        Key={'TokenKey': TOKEN_KEY}
    )
    if (dynamo_response["ResponseMetadata"]["HTTPStatusCode"] != 200):
        print("Failed to store token in database")
        print(f"DynamoDB Response: {dynamo_response}")
        return {
            'statusCode': 500,
            'body': 'Internal Server Error'
        }
    token_expiration = dynamo_response["Item"]["expiresIn"]
    token = dynamo_response["Item"]["token"]
    current_time = time.time()

    if token_expiration <= current_time:
        payload = {
            "tokenKey": TOKEN_KEY,
            "scope": SCOPE
        }
        token_payload = make_lambda_request("kroger-oauth", payload)
        if ("statusCode" not in token_payload or token_payload["statusCode"] != 200):
            print("Failed on invoking oauth token lambda")
            print(f"Oauth Lambda Response: {token_payload}")
            return {
                'statusCode': 500,
                'body': 'Internal Server Error'
            }
        token = token_payload["token"]

    return {'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive', 
            'Authorization': f'Bearer {token}',
            'Cache-Control': 'no-chache'}

def lambda_handler(event, context):
    print("received event: " + json.dumps(event, indent=2))
    if "queryStringParameters" not in event:
        return {
            'statusCode': 400,
            'body': 'Invalid request. Missing query parameters'
        }
    zipcode = event["queryStringParameters"].get("zipcode")
    radiusInMiles = event["queryStringParameters"].get("radiusInMiles")
    limit = event["queryStringParameters"].get("limit")
    if not zipcode or not radiusInMiles or not limit:
        return {
            'statusCode': 400,
            'body': 'Invalid request. zipcode, radiusInMiles, and limit are requered query parameters'
        }
    auth_header = get_oauth_header()
    locations_url = BASE_KROGER_URL + f"locations?filter.zipCode.near={zipcode}&filter.radiusInMiles={radiusInMiles}&filter.limit={limit}"
    try:
        response = requests.get(url=locations_url, headers=auth_header)
    except Exception as e:
        print(f"Failed to obtain location info {str(e)}")
        return {
            'statusCode': 500,
            'body': 'Internal Server Error'
        }
    stores_response_json = response.json()
    if "data" in stores_response_json:
        payload = {
            "zipcode": str(zipcode)
        }
        zipcode_payload = make_lambda_request("zipcode-latlongcoords", payload)
        for store in stores_response_json['data']:
            if "statusCode" in zipcode_payload and zipcode_payload["statusCode"] == 200:
                store["distance"] = get_distance(store=store, 
                                                zipcode_lat=zipcode_payload["body"]["latitude"],
                                                zipcode_long= zipcode_payload["body"]["longitude"])
            store["thumbnail"] = get_thumbnail(store["chain"])
        locations_response = LocationsResponse(zipcode=zipcode,
                                            radiusInMiles=radiusInMiles,
                                            limit=limit,
                                            stores=stores_response_json['data'])
        return {
            'statusCode': 200,
            'body': json.dumps(locations_response.to_dict()),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': "https://www.coleharris.dev",
                'Access-Control-Allow-Methods': 'GET',
                'Access-Control-Allow-Headers': 'Content-Type',
            }
        }
    
    print(f"Invalid locations info from kroger {stores_response_json}")
    return {
            'statusCode': 500,
            'body': 'Internal Server Error'
    }


