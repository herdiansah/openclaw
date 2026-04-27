#!/usr/bin/env node
/**
 * RTK Exec Wrapper for OpenClaw
 * Wraps exec commands with RTK for token optimization
 */

import { exec } from "node:child_process";
import { promisify } from "node:util";

const execAsync = promisify(exec);

// Commands that benefit from RTK
const RTK_COMMANDS = [
  "git",
  "cargo test",
  "npm test",
  "yarn test",
  "pnpm test",
  "docker ps",
  "docker logs",
  "docker images",
  "kubectl",
  "ls",
  "cat",
  "find",
  "grep",
  "rg",
];

// RTK binary path
const RTK_PATH = process.env.RTK_PATH || `${process.env.HOME || "/root"}/.local/bin/rtk`;

/**
 * Check if command should use RTK
 */
function shouldUseRTK(command) {
  const cmd = command.trim().toLowerCase();
  return RTK_COMMANDS.some((rtkCmd) => cmd.startsWith(rtkCmd) || cmd.includes(` ${rtkCmd} `));
}

/**
 * Execute command with optional RTK wrapper
 */
export async function execWithRTK(command, options = {}) {
  const useRTK = options.useRTK !== false && shouldUseRTK(command);
  
  const actualCommand = useRTK ? `${RTK_PATH} ${command}` : command;
  
  try {
    const { stdout, stderr } = await execAsync(actualCommand, {
      timeout: options.timeout || 30000,
      maxBuffer: options.maxBuffer || 10 * 1024 * 1024,
      ...options,
    });
    
    return {
      ok: true,
      stdout,
      stderr,
      command: actualCommand,
      usedRTK: useRTK,
    };
  } catch (error) {
    // Fallback: try without RTK if RTK fails
    if (useRTK && error.message.includes("rtk")) {
      console.error(`RTK failed for "${command}", falling back to raw execution...`);
      try {
        const { stdout, stderr } = await execAsync(command, {
          timeout: options.timeout || 30000,
          maxBuffer: options.maxBuffer || 10 * 1024 * 1024,
          ...options,
        });
        
        return {
          ok: true,
          stdout,
          stderr,
          command,
          usedRTK: false,
          fallback: true,
        };
      } catch (fallbackError) {
        return {
          ok: false,
          error: fallbackError.message,
          command,
          usedRTK: false,
          fallback: true,
        };
      }
    }
    
    return {
      ok: false,
      error: error.message,
      command: actualCommand,
      usedRTK: useRTK,
    };
  }
}

/**
 * Register RTK wrapper with OpenClaw exec tool
 */
export default {
  id: "rtk-exec-wrapper",
  register(api) {
    console.log("[RTK] Exec wrapper registered");
    
    // Wrap exec tool calls
    api.on("exec", (event) => {
      const command = event?.command;
      if (!command) return;
      
      if (shouldUseRTK(command)) {
        console.log(`[RTK] Will optimize command: ${command}`);
      }
    });
  },
};

// CLI for testing
if (process.argv[1]?.includes("rtk-exec-wrapper")) {
  const command = process.argv.slice(2).join(" ");
  if (!command) {
    console.error("Usage: node rtk-exec-wrapper.mjs <command>");
    process.exit(1);
  }
  
  execWithRTK(command).then((result) => {
    console.log("Result:", result);
  });
}
