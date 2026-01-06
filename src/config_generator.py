#!/usr/bin/env python3
"""
Interactive TOML Configuration Generator
Creates TOML config files through a user-friendly CLI interface.
"""

from pathlib import Path
from typing import Any, Dict, List, Union
import tomlkit  # For writing TOML files


class ConfigOption:
    """Represents a single configuration option."""

    def __init__(
        self,
        key: str,
        prompt: str,
        option_type: str = "string",
        choices: List[Any] = [],
        default: Any = None,
        required: bool = True,
        description: str = "Description not provided",
    ):
        """
        Args:
            key: TOML key name
            prompt: Question to display to user
            option_type: Type of value (string, int, float, bool, list)
            choices: List of valid choices (for selection)
            default: Default value if user presses Enter
            required: Whether this field is mandatory
            description: Additional help text
        """
        self.key = key
        self.prompt = prompt
        self.option_type = option_type
        self.choices = choices
        self.default = default
        self.required = required
        self.description = description


class ConfigGenerator:
    """Interactive TOML configuration file generator."""

    def __init__(self, sections: Dict[str, List[ConfigOption]]):
        """
        Args:
            sections: Dict mapping section names to lists of ConfigOptions
        """
        self.sections = sections
        self.config: dict[str, Any] = {}

    def _display_choices(self, choices: List[Any]) -> None:
        """Display numbered choices to user."""
        print("\nAvailable options:")
        for idx, choice in enumerate(choices, 1):
            print(f"  {idx}. {choice}")

    def _get_input(self, option: ConfigOption) -> Any:
        """Get and validate user input for a single option."""
        # Display description if available
        if option.description:
            print(f"\n💡 {option.description}")

        # Build prompt text
        prompt_text = option.prompt
        if option.default is not None:
            prompt_text += f" [default: {option.default}]"
        prompt_text += ": "

        # Display choices if available
        if option.choices:
            self._display_choices(option.choices)
            prompt_text = "Enter number or value: "

        while True:
            user_input = input(prompt_text).strip()

            # Handle empty input
            if not user_input:
                if option.default is not None:
                    return option.default
                elif not option.required:
                    return None
                else:
                    print("❌ This field is required. Please enter a value.")
                    continue

            # Handle choice selection
            if option.choices:
                try:
                    # Try numeric selection first
                    if user_input.isdigit():
                        idx = int(user_input) - 1
                        if 0 <= idx < len(option.choices):
                            return option.choices[idx]
                        else:
                            print(
                                f"❌ Please enter a number between 1 and {len(option.choices)}"
                            )
                            continue
                    # Allow direct value entry if it matches a choice
                    elif user_input in [str(c) for c in option.choices]:
                        return user_input
                    else:
                        print(
                            f"❌ Invalid choice. Please select from the list."
                        )
                        continue
                except ValueError:
                    print(f"❌ Invalid input.")
                    continue

            # Type conversion and validation
            try:
                if option.option_type == "int":
                    return int(user_input)
                elif option.option_type == "float":
                    return float(user_input)
                elif option.option_type == "bool":
                    return user_input.lower() in ["true", "yes", "y", "1"]
                elif option.option_type == "list":
                    # Parse comma-separated list
                    return [item.strip() for item in user_input.split(",")]
                else:  # string
                    return user_input
            except ValueError:
                print(f"❌ Invalid {option.option_type}. Please try again.")

    def run(self) -> Dict[str, Any]:
        """Run the interactive configuration process."""
        print("=" * 60)
        print("🔧 Interactive TOML Configuration Generator")
        print("=" * 60)

        for section_name, options in self.sections.items():
            print(f"\n{'─' * 60}")
            print(f"📋 Section: {section_name.upper()}")
            print(f"{'─' * 60}")

            section_config = {}
            for option in options:
                value = self._get_input(option)
                if value is not None:
                    section_config[option.key] = value

            if section_config:
                self.config[section_name] = section_config

        return self.config

    def save(self, output_path: Union[str, Path]) -> None:
        """Save configuration to TOML file."""
        output_path = Path(output_path)

        # Create directory if it doesn't exist
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            tomlkit.dump(self.config, f)

        print(f"\n✅ Configuration saved to: {output_path.absolute()}")

    def preview(self) -> None:
        """Display the generated configuration."""
        print("\n" + "=" * 60)
        print("📄 Generated Configuration Preview")
        print("=" * 60)
        print(tomlkit.dumps(self.config))


# Example configuration for po_sma script
def create_po_sma_config():
    """Define configuration options for po_sma script."""

    sections = {
        "general": [
            ConfigOption(
                key="portfolio_id",
                prompt="Enter portfolio ID",
                option_type="string",
                required=True,
                description="Unique identifier for the portfolio",
            ),
            ConfigOption(
                key="strategy_name",
                prompt="Enter strategy name",
                option_type="string",
                default="sma_strategy",
                description="Name of the trading strategy",
            ),
        ],
        "data": [
            ConfigOption(
                key="data_source",
                prompt="Select data source",
                choices=["s3", "local", "database", "api"],
                default="s3",
                description="Where to fetch market data from",
            ),
            ConfigOption(
                key="date_range_days",
                prompt="Enter number of days for historical data",
                option_type="int",
                default=252,
                description="Trading days for backtesting period",
            ),
            ConfigOption(
                key="symbols",
                prompt="Enter comma-separated list of symbols",
                option_type="list",
                default=["SPY", "QQQ"],
                description="Securities to analyze",
            ),
        ],
        "strategy": [
            ConfigOption(
                key="short_window",
                prompt="Enter short SMA window",
                option_type="int",
                default=50,
                description="Days for short moving average",
            ),
            ConfigOption(
                key="long_window",
                prompt="Enter long SMA window",
                option_type="int",
                default=200,
                description="Days for long moving average",
            ),
            ConfigOption(
                key="rebalance_frequency",
                prompt="Select rebalancing frequency",
                choices=["daily", "weekly", "monthly", "quarterly"],
                default="monthly",
            ),
        ],
        "risk": [
            ConfigOption(
                key="max_position_size",
                prompt="Enter maximum position size (0-1)",
                option_type="float",
                default=0.25,
                description="Maximum allocation per security (fraction of portfolio)",
            ),
            ConfigOption(
                key="stop_loss_pct",
                prompt="Enter stop loss percentage",
                option_type="float",
                default=0.10,
                description="Exit position if loss exceeds this threshold",
            ),
            ConfigOption(
                key="enable_var_calculation",
                prompt="Enable VaR calculation? (yes/no)",
                option_type="bool",
                default=True,
            ),
        ],
        "output": [
            ConfigOption(
                key="output_dir",
                prompt="Enter output directory path",
                option_type="string",
                default="./results",
                description="Where to save analysis results",
            ),
            ConfigOption(
                key="save_charts",
                prompt="Save visualization charts? (yes/no)",
                option_type="bool",
                default=True,
            ),
        ],
    }

    return sections


def main():
    """Main entry point."""
    # Create the configuration sections for po_sma
    sections = create_po_sma_config()

    # Initialize generator
    generator = ConfigGenerator(sections)

    # Run interactive session
    config = generator.run()

    # Preview configuration
    generator.preview()

    # Confirm and save
    print("\n" + "=" * 60)
    save_choice = (
        input("Save this configuration? (yes/no) [yes]: ").strip().lower()
    )

    if save_choice in ["", "yes", "y"]:
        default_path = "./configs/po_sma_config.toml"
        custom_path = input(f"Enter save path [{default_path}]: ").strip()
        output_path = custom_path or default_path

        generator.save(output_path)
        print("\n🎉 Configuration complete!")
    else:
        print("\n❌ Configuration not saved.")


if __name__ == "__main__":
    main()
