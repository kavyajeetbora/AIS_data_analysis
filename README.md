# Ship route segregation on raw AIS data
Segregation of ship traffic in Denmark waters using AIS Data. 

Raw AIS Data:
![Raw AIS data](https://user-images.githubusercontent.com/38955297/193509172-1a0d6301-2eeb-499b-b160-7b65c9a62aef.png)

Ships were segregated based on the checkpoints:

Checkpoints:
![checkpoints](https://user-images.githubusercontent.com/38955297/193509330-f6806cc6-58ca-4f54-9411-d4c021b2f977.png)

Based on the checkpoints the ships where segregated. Ships travelling through all the available checkpoints as shown above was filtered out from the raw data. 

This is how the segragated data looked like:

<img width="900" alt="ship route segregation" src="https://user-images.githubusercontent.com/38955297/193509483-459cca84-bdf3-4502-9854-74796aaab116.PNG">

Python was used for achieving this along with python libraries geopandas, plotly and folium for plotting




