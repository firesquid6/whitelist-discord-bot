import {
  Client,
  GatewayIntentBits,
  REST,
  Routes,
  SlashCommandBuilder,
} from "discord.js";

const DISCORD_TOKEN = process.env.DISCORD_TOKEN!;
const CLIENT_ID = process.env.CLIENT_ID!;
const PROCESS_PORT = process.env.PROCESS_PORT!;

if (!DISCORD_TOKEN || !CLIENT_ID || !PROCESS_PORT) {
  console.error("Missing required env vars: DISCORD_TOKEN, CLIENT_ID, PROCESS_PORT");
  process.exit(1);
}

const commands = [
  new SlashCommandBuilder()
    .setName("javawhitelist")
    .setDescription("Add a player to the Java whitelist")
    .addStringOption((opt) =>
      opt.setName("name").setDescription("Minecraft username").setRequired(true)
    ),
  new SlashCommandBuilder()
    .setName("bedrockwhitelist")
    .setDescription("Add a player to the Bedrock whitelist")
    .addStringOption((opt) =>
      opt.setName("name").setDescription("Minecraft username").setRequired(true)
    ),
];

// Register slash commands
const rest = new REST().setToken(DISCORD_TOKEN);
await rest.put(Routes.applicationCommands(CLIENT_ID), {
  body: commands.map((c) => c.toJSON()),
});
console.log("Registered slash commands");

// Start bot
const client = new Client({ intents: [GatewayIntentBits.Guilds] });

client.on("ready", () => {
  console.log(`Logged in as ${client.user?.tag}`);
});

client.on("interactionCreate", async (interaction) => {
  if (!interaction.isChatInputCommand()) return;

  const name = interaction.options.getString("name", true);
  let serverCommand: string;
  let label: string;

  if (interaction.commandName === "javawhitelist") {
    serverCommand = `whitelist add ${name}`;
    label = "Java";
  } else if (interaction.commandName === "bedrockwhitelist") {
    serverCommand = `fwhitelist add ${name}`;
    label = "Bedrock";
  } else {
    return;
  }

  try {
    const res = await fetch(`http://localhost:${PROCESS_PORT}/stdin`, {
      method: "POST",
      body: serverCommand,
    });

    if (!res.ok) {
      await interaction.reply(`Failed to send command (HTTP ${res.status})`);
      return;
    }

    await interaction.reply(`Added \`${name}\` to the ${label} whitelist`);
  } catch (err) {
    await interaction.reply(`Error sending command: ${err}`);
  }
});

client.login(DISCORD_TOKEN);
