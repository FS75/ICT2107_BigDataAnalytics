import { Card, Grid, Metric, Text } from "@tremor/react";

/*
  Keep in mind that every card component's children should
  match accordingly the object properties that is passed into it
  eg. here we are explicitly using item.name and item.desc because
  we know that the data passed over here is only for companies.

  Having generic cards would be too troublesome as too much logic will 
  be involved because everything has different properties
*/
export default function CompanyCard({data}) {
  return (
    <Grid numColsSm={2} numColsLg={3} className="gap-6">
      {data.map((item) => (
        <Card key={item.name}>
          <Text>{item.name}</Text>
          <Metric>{item.desc}</Metric>
        </Card>
      ))}
    </Grid>
  );
}