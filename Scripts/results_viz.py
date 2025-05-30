import sys
import os
import pandas as pd
from plotnine import ggplot, aes, geom_line, geom_point, scale_shape_manual, theme_minimal, scale_linetype_manual, scale_size_manual, theme, element_rect, labs, element_text, geom_text
import numpy as np

# Add the Optimization directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'Optimization'))
from config import RESULT_PATH

make_figures = True

for config_name in ["5_2"]:
    # Read data from CSV files
    choicestats_df = pd.read_csv(os.path.join(RESULT_PATH, f'{config_name}_choicestats.csv'))
    nsmstats_df = pd.read_csv(os.path.join(RESULT_PATH, f'{config_name}_nsmstats.csv'))
    modesplit_df = pd.read_csv(os.path.join(RESULT_PATH, f'{config_name}_modesplit.csv'))

    # Print column names for verification
    print(f"\nColumns in {config_name} choicestats_df:", choicestats_df.columns.tolist())
    print(f"Columns in {config_name} nsmstats_df:", nsmstats_df.columns.tolist())
    print(f"Columns in {config_name} modesplit_df:", modesplit_df.columns.tolist())

    # Map mode numbers to text labels
    mode_mapping = {0: 'walk', 1: 'bike', 2: 'car', 3: 'pt', 4: 'nsm'}
    modesplit_df['mode'] = modesplit_df['mode'].map(mode_mapping)

    # Scale B_TIME values (convert from hours to minutes)
    choicestats_df.loc[choicestats_df['stat'] == 'B_TIME', 'smoothed'] *= 60

    # create more specific dfs for plotting
    ASC_df = choicestats_df[choicestats_df['stat'].isin(['ASC_WALK','ASC_BIKE','ASC_CAR','ASC_PT','ASC_NSM'])]
    bval_df = choicestats_df[choicestats_df['stat'].isin(['B_COST','B_TIME','B_RISK'])]
    nsm_df = nsmstats_df[nsmstats_df['stat'].isin(['occupancy','service_rate','nsm_car_time_ratio'])]
    wait_df = nsmstats_df[nsmstats_df['stat'].isin(['nsm_wait_time'])]

    # Print column names for filtered dataframes
    print(f"\nColumns in {config_name} ASC_df:", ASC_df.columns.tolist())
    print(f"Columns in {config_name} bval_df:", bval_df.columns.tolist())
    print(f"Columns in {config_name} nsm_df:", nsm_df.columns.tolist())
    print(f"Columns in {config_name} wait_df:", wait_df.columns.tolist())
    print(f"Columns in {config_name} modesplit_df:", modesplit_df.columns.tolist())

    # Function to get last points for annotation
    def get_last_points(df, y_col='smoothed'):
        group_col = 'stat' if 'stat' in df.columns else 'mode'
        # Find the last non-null value for each group
        last_points = df.sort_values('iter').groupby(group_col, group_keys=False).apply(
            lambda x: x[x[y_col].notna()].iloc[-1] if not x[y_col].isna().all() else None
        )
        return last_points

    # Get last points for each plot
    ASC_last = get_last_points(ASC_df)
    bval_last = get_last_points(bval_df)
    nsm_last = get_last_points(nsm_df)
    wait_last = get_last_points(wait_df)
    mode_last = get_last_points(modesplit_df, 'ratio')

    # Define line types and sizes for each Stat
    line_types = {'ASC_WALK': 'solid', 'ASC_BIKE': 'solid', 'ASC_CAR': 'dotted',
                'ASC_PT': 'dotted', 'ASC_NSM': 'solid', 'B_COST': 'dashdot', 'B_TIME': 'dotted', 'service_rate': 'solid'}

    line_sizes = {'ASC_WALK': 0.5, 'ASC_BIKE': 0.5, 'ASC_CAR': 0.5,
                'ASC_PT': 0.75, 'ASC_NSM': 3.0, 'B_COST': 0.75, 'B_TIME': 1.0, 'service_rate': 1.5}

    # Create a theme with white background and larger text
    white_theme = theme_minimal() + theme(
        panel_background=element_rect(fill='white'),
        plot_background=element_rect(fill='white'),
        axis_title=element_text(size=14),  # Larger axis titles
        axis_text=element_text(size=12),   # Larger axis numbers
        legend_title=element_text(size=14), # Larger legend title
        legend_text=element_text(size=12)   # Larger legend text
    )

    ASCplot = (
        ggplot(ASC_df, aes(x='iter', y='smoothed', group='stat', color='stat'))
        + geom_line(size=1.0)  # Thicker lines
        + geom_point(size=1.0)  # Keep points the same size
        + geom_text(data=ASC_last, mapping=aes(x='iter', y='smoothed', label='smoothed.round(2)'), 
                   nudge_x=1.5, size=12, show_legend=False)  # Add labels for last points
        + labs(y='value (smoothed)')  # Update y-axis label
        + white_theme
    )
    # Save the plot to a PNG file
    ASCplot.save(filename=os.path.join(RESULT_PATH, f"{config_name}_ASC.png"), dpi=300, height=6, width=8, units='in')

    BVALplot = (
        ggplot(bval_df, aes(x='iter', y='smoothed', group='stat', color='stat'))
        + geom_line(size=1.0)  # Thicker lines
        + geom_point(size=1.0)  # Keep points the same size
        + geom_text(data=bval_last, mapping=aes(x='iter', y='smoothed', label='smoothed.round(2)'), 
                   nudge_x=1.5, size=12, show_legend=False)  # Add labels for last points
        + labs(y='value (smoothed)')  # Update y-axis label
        + white_theme
    )
    # Save the plot to a PNG file
    BVALplot.save(filename=os.path.join(RESULT_PATH, f"{config_name}_BVAL.png"), dpi=300, height=6, width=8, units='in')

    NSMplot = (
        ggplot(nsm_df, aes(x='iter', y='smoothed', group='stat', color='stat'))
        + geom_line(size=1.0)  # Thicker lines
        + geom_point(size=1.0)  # Keep points the same size
        + geom_text(data=nsm_last, mapping=aes(x='iter', y='smoothed', label='smoothed.round(2)'), 
                   nudge_x=1.5, size=12, show_legend=False)  # Add labels for last points
        + labs(y='value (smoothed)')  # Update y-axis label
        + white_theme
    )
    # Save the plot to a PNG file
    NSMplot.save(filename=os.path.join(RESULT_PATH, f"{config_name}_NSM.png"), dpi=300, height=6, width=8, units='in')

    WAITplot = (
        ggplot(wait_df, aes(x='iter', y='smoothed', group='stat', color='stat'))
        + geom_line(size=1.0)  # Thicker lines
        + geom_point(size=1.0)  # Keep points the same size
        + geom_text(data=wait_last, mapping=aes(x='iter', y='smoothed', label='smoothed.round(1)'), 
                   nudge_x=1.5, size=12, show_legend=False)  # Add labels for last points
        + labs(y='value (smoothed)')  # Update y-axis label
        + white_theme
    )
    # Save the plot to a PNG file
    WAITplot.save(filename=os.path.join(RESULT_PATH, f"{config_name}_WAIT.png"), dpi=300, height=6, width=8, units='in')

    mode_plot = (
        ggplot(modesplit_df, aes(x='iter', y='ratio', group='mode', color='mode'))
        + geom_line(size=1.0)  # Thicker lines
        + geom_point(size=1.0)  # Keep points the same size
        + geom_text(data=mode_last, mapping=aes(x='iter', y='ratio', label='ratio.round(2)'), 
                   nudge_x=1.5, size=12, show_legend=False)  # Add labels for last points
        + labs(y='ratio')  # Keep original y-axis label for modesplit
        + white_theme
    )
    # Save the plot to a PNG file
    mode_plot.save(filename=os.path.join(RESULT_PATH, f"{config_name}_MODESPLIT.png"), dpi=300, height=6, width=8, units='in')

