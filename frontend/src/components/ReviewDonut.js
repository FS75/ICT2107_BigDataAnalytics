import { Card, Title, DonutChart } from "@tremor/react";

const valueFormatter = (number: number) =>
  `${Intl.NumberFormat("us").format(number).toString()}`;

export default function Donut({data}) {
    return (
        <Card className="max-w-lg">
            <Title>{data.name}</Title>
            <DonutChart
            className="mt-6"
            data={data.overallSentiment}
            category="sentiment"
            index="type"
            valueFormatter={valueFormatter}
            colors={["lime", "rose", "neutral"]}
            />
        </Card>
    )
};