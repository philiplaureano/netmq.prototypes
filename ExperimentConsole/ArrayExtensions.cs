using System;
using System.Collections.Generic;
using System.Linq;

namespace ExperimentConsole
{
    public static class ArrayExtensions
    {
        public static TItem GetRandomElement<TItem>(this IEnumerable<TItem> items)
        {
            var random = new Random();
            var itemsAsArray = items.ToArray();

            return itemsAsArray.Length == 0 ? default(TItem) : itemsAsArray[random.Next(0, itemsAsArray.Length)];
        }
    }
}