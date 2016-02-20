sqladmin
========

A free, open source alternative to a [plugin][overpriced-crap] with the same name that costs $2.50 more.

![](http://i.imgur.com/bZJ00Mp.png)

Oh, and it's under 200 lines *before* being compiled. Didn't take very long to make either. It even uses HikariCP for a whopping speed boost!

## Notes

If you are simply looking for a database management tool I highly recommend using [MySQL Workbench][awesome-sauce] or [phpMyAdmin][less-awesome-sauce-but-still-saucy].
They are both fantastic pieces of software and will easily be better than any plugin anybody could ever make.

## Compilation

Compilation is simple and can be done on almost any machine. Must have JDK8 installed though.

```
git clone git@github.com:Fireflies/sqladmin.git
cd sqladmin
./gradlew shadowJar
```

You can find the output JAR in `build/libs/`.

## Disclaimer

I am not responsible for any damages done with this plugin, don't want things to be screwed up?
Don't give out the permission node!

[overpriced-crap]: https://www.spigotmc.org/resources/sqladmin.6857/
[awesome-sauce]: https://www.mysql.com/products/workbench/
[less-awesome-sauce-but-still-saucy]: https://www.phpmyadmin.net/