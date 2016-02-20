package ninja.smirking.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;

import com.zaxxer.hikari.HikariDataSource;
import org.bukkit.ChatColor;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.plugin.java.JavaPlugin;

public class SqlPlugin extends JavaPlugin {
    private HikariDataSource source;

    @Override
    public void onEnable() {
        saveDefaultConfig();

        source = new HikariDataSource();
        source.setJdbcUrl(String.format("jdbc:mysql://%s/%s", getConfig().getString("host", "localhost"), getConfig().getString("name", "minecraft")));
        source.setUsername(getConfig().getString("user", "minecraft"));
        source.setPassword(getConfig().getString("pass", "notch"));
    }

    @Override
    public void onDisable() {
        if (!source.isClosed()) {
            source.close();
        }
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (args.length < 1) {
            sender.sendMessage(ChatColor.RED + "Try use some args, mate.");
        } else {
            switch (args[0].toLowerCase()) {
                case "test":
                    lazilyExecute(() -> {
                        try (Connection connection = source.getConnection()) {
                            if (connection.isValid(3)) {
                                sender.sendMessage(ChatColor.GREEN + "Ding dong, works");
                            } else {
                                throw new Exception();
                            }
                        } catch (Exception e) {
                            sender.sendMessage(ChatColor.RED + "Oh noes, connection buggered up");
                        }
                    });
                    break;
                case "query":
                    if (args.length < 2) {
                        sender.sendMessage("You're gonna have to try harder than that.");
                        break;
                    }
                    lazilyExecute(sender, String.join(" ", Arrays.copyOfRange(args, 1, args.length)), args[1].toUpperCase().equals("SELECT") ? result -> {
                        int cols = result.getMetaData().getColumnCount();
                        sender.sendMessage("Results: ");
                        while (result.next()) {
                            sender.sendMessage("  Result: ");
                            for(int i = 1; i <= cols; i++) {
                                sender.sendMessage(String.format("    %s -> \"%s\"", result.getMetaData().getColumnName(i), result.getString(i)));
                            }
                        }
                    } : null);
                    break;
                case "tables":
                    dump(sender, "Tables", 3, con -> con.getMetaData().getTables(null, null, "%s", null));
                    break;
                case "databases":
                    dump(sender, "Databases", 1, con -> con.getMetaData().getCatalogs());
                    break;
            }
        }
        return true;
    }

    void lazilyExecute(CommandSender sender, String query, UnsafeConsumer<ResultSet> consumer) {
        lazilyExecute(() -> {
            try (Connection connection = source.getConnection(); Statement statement = connection.createStatement()) {
                if (consumer == null) {
                    int affected = statement.executeUpdate(query);
                    sender.sendMessage(ChatColor.GREEN + String.format("Query OK, %d rows affected.", affected));
                } else {
                    try (ResultSet set = statement.executeQuery(query)) {
                        if (set != null) {
                            consumer.accept(set);
                        }
                    }
                }
            } catch (Exception e) {
                sender.sendMessage(ChatColor.RED + e.getMessage());
            }
        });
    }

    void lazilyExecute(Runnable task) {
        getServer().getScheduler().runTaskAsynchronously(this, task);
    }

    void dump(CommandSender sender, String what, int columnIndex, UnsafeFunction<Connection, ResultSet> supplier) {
        try (Connection connection = source.getConnection(); ResultSet set = supplier.get(connection)) {
            sender.sendMessage(String.format("%s: ", what));
            while (set.next()) {
                sender.sendMessage(String.format("  %s", set.getString(columnIndex)));
            }
        } catch (Exception e) {
            sender.sendMessage(ChatColor.RED + "I am in ur dbez selektin yr " + what.toLowerCase().replace("es", "ez"));
            sender.sendMessage(ChatColor.RED + e.getMessage());
        }
    }

    @FunctionalInterface
    interface UnsafeFunction<I, O> {
        O get(I i) throws Exception;
    }

    @FunctionalInterface
    interface UnsafeConsumer<T> {
        void accept(T t) throws Exception;
    }
}
